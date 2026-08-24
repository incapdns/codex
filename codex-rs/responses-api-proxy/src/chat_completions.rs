use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::io;
use std::pin::Pin;

use axum::body::Body;
use axum::body::Bytes;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use eventsource_stream::Eventsource;
use futures::Stream;
use futures::StreamExt;
use serde::Serialize;
use serde_json::Map;
use serde_json::Value;

use crate::chatgpt::ChatgptState;
use crate::dump::ExchangeDump;
use crate::routes::resolve_responses_route;

const CHAT_COMPLETIONS_PATH: &str = "/v1/chat/completions";

pub(crate) async fn handle(state: ChatgptState, headers: HeaderMap, body: Bytes) -> Response {
    let exchange_dump = state.dump_dir.as_deref().and_then(|dump_dir| {
        dump_dir
            .dump_http_request(&http::Method::POST, CHAT_COMPLETIONS_PATH, &headers, &body)
            .map_err(|err| {
                eprintln!("responses-api-proxy failed to dump request: {err}");
                err
            })
            .ok()
    });
    let translated = match translate_request(&body) {
        Ok(translated) => translated,
        Err(err) => return invalid_request_response(err),
    };
    let Some(route) = resolve_responses_route("POST", "/v1/responses") else {
        return proxy_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Responses create route is unavailable".to_string(),
        );
    };
    let upstream =
        match crate::chatgpt::forward_request(&state, &route, headers, translated.body).await {
            Ok(response) => response,
            Err(err) => {
                eprintln!("forwarding Chat Completions compatibility request failed: {err:#}");
                return proxy_error_response(StatusCode::BAD_GATEWAY, err.to_string());
            }
        };
    if !upstream.status().is_success() {
        return crate::chatgpt::upstream_response(upstream, exchange_dump);
    }

    if translated.client_stream {
        streaming_response(
            upstream,
            translated.model,
            translated.include_usage,
            translated.logprobs_requested,
            translated.legacy_function_names,
            exchange_dump,
        )
    } else {
        match collect_completion(
            upstream,
            &translated.model,
            translated.logprobs_requested,
            &translated.legacy_function_names,
        )
        .await
        {
            Ok((headers, body)) => buffered_response(headers, body, exchange_dump),
            Err(err) => proxy_error_response(StatusCode::BAD_GATEWAY, err.to_string()),
        }
    }
}

#[derive(Debug)]
struct TranslatedRequest {
    body: Bytes,
    model: String,
    client_stream: bool,
    include_usage: bool,
    logprobs_requested: bool,
    legacy_function_names: HashSet<String>,
}

#[derive(Debug)]
struct CompatError {
    message: String,
    param: Option<String>,
    code: &'static str,
}

impl CompatError {
    fn invalid(param: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            param: Some(param.into()),
            code: "invalid_parameter",
        }
    }

    fn unsupported(param: impl Into<String>) -> Self {
        let param = param.into();
        Self {
            message: format!(
                "The Chat Completions compatibility layer cannot translate `{param}` to the Responses API"
            ),
            param: Some(param),
            code: "unsupported_parameter",
        }
    }
}

impl std::fmt::Display for CompatError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for CompatError {}

fn translate_request(body: &[u8]) -> Result<TranslatedRequest, CompatError> {
    let payload: Value = serde_json::from_slice(body)
        .map_err(|err| CompatError::invalid("body", format!("Invalid JSON body: {err}")))?;
    let request = payload
        .as_object()
        .ok_or_else(|| CompatError::invalid("body", "Request body must be a JSON object"))?;
    for field in request.keys() {
        if !matches!(
            field.as_str(),
            "audio"
                | "frequency_penalty"
                | "function_call"
                | "functions"
                | "logit_bias"
                | "logprobs"
                | "max_completion_tokens"
                | "max_tokens"
                | "messages"
                | "metadata"
                | "modalities"
                | "model"
                | "moderation"
                | "n"
                | "parallel_tool_calls"
                | "prediction"
                | "presence_penalty"
                | "prompt_cache_key"
                | "prompt_cache_options"
                | "prompt_cache_retention"
                | "reasoning_effort"
                | "response_format"
                | "safety_identifier"
                | "seed"
                | "service_tier"
                | "stop"
                | "store"
                | "stream"
                | "stream_options"
                | "temperature"
                | "tool_choice"
                | "tools"
                | "top_logprobs"
                | "top_p"
                | "user"
                | "verbosity"
                | "web_search_options"
        ) {
            return Err(CompatError::invalid(
                field,
                format!("Unknown parameter `{field}`"),
            ));
        }
    }
    let model = request
        .get("model")
        .and_then(Value::as_str)
        .filter(|model| !model.is_empty())
        .ok_or_else(|| CompatError::invalid("model", "`model` must be a non-empty string"))?
        .to_string();
    let messages = request
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(|| CompatError::invalid("messages", "`messages` must be an array"))?;
    let client_stream = optional_bool(request, "stream")?.unwrap_or(false);
    let include_usage = request
        .get("stream_options")
        .and_then(Value::as_object)
        .and_then(|options| options.get("include_usage"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if let Some(options) = request
        .get("stream_options")
        .filter(|value| !value.is_null())
    {
        if !client_stream {
            return Err(CompatError::invalid(
                "stream_options",
                "`stream_options` may only be set when `stream` is true",
            ));
        }
        let options = options.as_object().ok_or_else(|| {
            CompatError::invalid("stream_options", "`stream_options` must be an object")
        })?;
        for (name, value) in options {
            if !matches!(name.as_str(), "include_obfuscation" | "include_usage") {
                return Err(CompatError::invalid(
                    format!("stream_options.{name}"),
                    format!("Unknown parameter `stream_options.{name}`"),
                ));
            }
            if !matches!(value, Value::Bool(_)) {
                return Err(CompatError::invalid(
                    format!("stream_options.{name}"),
                    format!("`stream_options.{name}` must be a boolean"),
                ));
            }
        }
    }

    if optional_bool(request, "store")? == Some(true) {
        return Err(CompatError::invalid(
            "store",
            "`store: true` is unavailable because the ChatGPT Responses backend requires `store: false`",
        ));
    }
    if let Some(n) = request.get("n").filter(|value| !value.is_null()) {
        let n = n
            .as_u64()
            .ok_or_else(|| CompatError::invalid("n", "`n` must be a positive integer"))?;
        if n != 1 {
            return Err(CompatError::invalid(
                "n",
                "Only `n: 1` is supported by the Responses API compatibility layer",
            ));
        }
    }
    for unsupported in [
        "audio",
        "frequency_penalty",
        "logit_bias",
        "prediction",
        "presence_penalty",
        "seed",
        "stop",
    ] {
        if request
            .get(unsupported)
            .is_some_and(|value| !value.is_null())
        {
            return Err(CompatError::unsupported(unsupported));
        }
    }
    translate_modalities(request)?;

    let mut input = Vec::new();
    let mut custom_call_ids = HashSet::new();
    let explicit_call_ids = explicit_tool_call_ids(messages);
    let mut legacy_calls = HashMap::<String, VecDeque<String>>::new();
    for (index, message) in messages.iter().enumerate() {
        translate_message(
            message,
            index,
            &mut input,
            &mut custom_call_ids,
            &explicit_call_ids,
            &mut legacy_calls,
        )?;
    }

    let mut responses = Map::new();
    responses.insert("model".to_string(), Value::String(model.clone()));
    responses.insert("input".to_string(), Value::Array(input));
    responses.insert("store".to_string(), Value::Bool(false));
    // The ChatGPT backend is stream-oriented. Non-streaming Chat Completions are
    // assembled locally from the same upstream event stream.
    responses.insert("stream".to_string(), Value::Bool(true));

    for field in [
        "metadata",
        "parallel_tool_calls",
        "moderation",
        "prompt_cache_key",
        "prompt_cache_options",
        "prompt_cache_retention",
        "safety_identifier",
        "service_tier",
        "temperature",
        "top_p",
        "user",
    ] {
        copy_if_present(request, &mut responses, field, field);
    }
    let max_completion_tokens = request
        .get("max_completion_tokens")
        .filter(|value| !value.is_null());
    let legacy_max_tokens = request.get("max_tokens").filter(|value| !value.is_null());
    if let (Some(current), Some(legacy)) = (max_completion_tokens, legacy_max_tokens)
        && current != legacy
    {
        return Err(CompatError::invalid(
            "max_tokens",
            "`max_tokens` and `max_completion_tokens` cannot specify different values",
        ));
    }
    if let Some(max_tokens) = max_completion_tokens.or(legacy_max_tokens) {
        responses.insert("max_output_tokens".to_string(), max_tokens.clone());
    }
    if let Some(reasoning_effort) = request.get("reasoning_effort") {
        responses.insert(
            "reasoning".to_string(),
            serde_json::json!({"effort": reasoning_effort}),
        );
    }
    translate_text_config(request, &mut responses)?;
    translate_tools(request, &mut responses)?;
    translate_tool_choice(request, &mut responses)?;
    let logprobs_requested = translate_logprobs(request, &mut responses)?;
    let legacy_function_names = legacy_function_names(request);

    if let Some(stream_options) = request.get("stream_options").and_then(Value::as_object)
        && let Some(include_obfuscation) = stream_options.get("include_obfuscation")
    {
        responses.insert(
            "stream_options".to_string(),
            serde_json::json!({"include_obfuscation": include_obfuscation}),
        );
    }

    let body = serde_json::to_vec(&Value::Object(responses))
        .map(Bytes::from)
        .map_err(|err| CompatError::invalid("body", err.to_string()))?;
    Ok(TranslatedRequest {
        body,
        model,
        client_stream,
        include_usage,
        logprobs_requested,
        legacy_function_names,
    })
}

fn translate_modalities(request: &Map<String, Value>) -> Result<(), CompatError> {
    let Some(modalities) = request.get("modalities").filter(|value| !value.is_null()) else {
        return Ok(());
    };
    let modalities = modalities
        .as_array()
        .ok_or_else(|| CompatError::invalid("modalities", "`modalities` must be an array"))?;
    if modalities.is_empty() {
        return Err(CompatError::invalid(
            "modalities",
            "`modalities` must contain at least one output modality",
        ));
    }
    for (index, modality) in modalities.iter().enumerate() {
        match modality.as_str() {
            Some("text") => {}
            Some("audio") => {
                return Err(CompatError::unsupported(format!(
                    "modalities[{index}]=audio"
                )));
            }
            Some(value) => {
                return Err(CompatError::invalid(
                    format!("modalities[{index}]"),
                    format!("Unsupported output modality `{value}`"),
                ));
            }
            None => {
                return Err(CompatError::invalid(
                    format!("modalities[{index}]"),
                    "Output modalities must be strings",
                ));
            }
        }
    }
    Ok(())
}

fn translate_logprobs(
    request: &Map<String, Value>,
    responses: &mut Map<String, Value>,
) -> Result<bool, CompatError> {
    let requested = match request.get("logprobs") {
        None | Some(Value::Null) | Some(Value::Bool(false)) => false,
        Some(Value::Bool(true)) => true,
        Some(_) => {
            return Err(CompatError::invalid(
                "logprobs",
                "`logprobs` must be a boolean or null",
            ));
        }
    };
    let top_logprobs = request.get("top_logprobs").filter(|value| !value.is_null());
    if top_logprobs.is_some() && !requested {
        return Err(CompatError::invalid(
            "top_logprobs",
            "`logprobs` must be true when `top_logprobs` is set",
        ));
    }
    if let Some(value) = top_logprobs {
        let value = value.as_u64().filter(|value| *value <= 20).ok_or_else(|| {
            CompatError::invalid(
                "top_logprobs",
                "`top_logprobs` must be an integer between 0 and 20",
            )
        })?;
        responses.insert("top_logprobs".to_string(), Value::from(value));
    }
    if requested {
        responses.insert(
            "include".to_string(),
            serde_json::json!(["message.output_text.logprobs"]),
        );
    }
    Ok(requested)
}

fn optional_bool(request: &Map<String, Value>, field: &str) -> Result<Option<bool>, CompatError> {
    match request.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(CompatError::invalid(
            field,
            format!("`{field}` must be a boolean"),
        )),
    }
}

fn copy_if_present(
    source: &Map<String, Value>,
    destination: &mut Map<String, Value>,
    source_name: &str,
    destination_name: &str,
) {
    if let Some(value) = source.get(source_name).filter(|value| !value.is_null()) {
        destination.insert(destination_name.to_string(), value.clone());
    }
}

fn explicit_tool_call_ids(messages: &[Value]) -> HashSet<String> {
    messages
        .iter()
        .filter_map(Value::as_object)
        .filter_map(|message| message.get("tool_calls").and_then(Value::as_array))
        .flatten()
        .filter_map(|call| call.get("id").and_then(Value::as_str))
        .map(str::to_string)
        .collect()
}

fn synthetic_legacy_call_id(
    message_index: usize,
    kind: &str,
    explicit_call_ids: &HashSet<String>,
) -> String {
    let base = format!("chatcmpl_legacy_{kind}_{message_index}");
    if !explicit_call_ids.contains(&base) {
        return base;
    }
    let mut suffix = 1;
    loop {
        let candidate = format!("{base}_{suffix}");
        if !explicit_call_ids.contains(&candidate) {
            return candidate;
        }
        suffix += 1;
    }
}

fn translate_message(
    value: &Value,
    index: usize,
    input: &mut Vec<Value>,
    custom_call_ids: &mut HashSet<String>,
    explicit_call_ids: &HashSet<String>,
    legacy_calls: &mut HashMap<String, VecDeque<String>>,
) -> Result<(), CompatError> {
    let message = value.as_object().ok_or_else(|| {
        CompatError::invalid(
            format!("messages[{index}]"),
            "Each message must be an object",
        )
    })?;
    let role = message.get("role").and_then(Value::as_str).ok_or_else(|| {
        CompatError::invalid(
            format!("messages[{index}].role"),
            "Message role must be a string",
        )
    })?;
    if message.get("audio").is_some_and(|value| !value.is_null()) {
        return Err(CompatError::unsupported(format!("messages[{index}].audio")));
    }
    for field in message.keys() {
        let supported = match role {
            "assistant" => matches!(
                field.as_str(),
                "role" | "audio" | "content" | "function_call" | "name" | "refusal" | "tool_calls"
            ),
            "tool" => matches!(field.as_str(), "role" | "content" | "tool_call_id"),
            "function" => matches!(field.as_str(), "role" | "content" | "name"),
            "developer" | "system" | "user" => {
                matches!(field.as_str(), "role" | "content" | "name")
            }
            _ => false,
        };
        if !supported {
            return Err(CompatError::invalid(
                format!("messages[{index}].{field}"),
                format!("Unknown parameter `messages[{index}].{field}`"),
            ));
        }
    }

    if role == "function" {
        if !message.contains_key("content") {
            return Err(CompatError::invalid(
                format!("messages[{index}].content"),
                "Function message `content` is required",
            ));
        }
        let name = message.get("name").and_then(Value::as_str).ok_or_else(|| {
            CompatError::invalid(
                format!("messages[{index}].name"),
                "Function messages require a string `name`",
            )
        })?;
        let output = match message.get("content") {
            Some(Value::String(content)) => content.clone(),
            Some(Value::Null) => String::new(),
            _ => {
                return Err(CompatError::invalid(
                    format!("messages[{index}].content"),
                    "Function message `content` must be a string or null",
                ));
            }
        };
        let call_id = legacy_calls
            .get_mut(name)
            .and_then(VecDeque::pop_front)
            .unwrap_or_else(|| synthetic_legacy_call_id(index, "result", explicit_call_ids));
        if !input.iter().any(|item| {
            item.get("type").and_then(Value::as_str) == Some("function_call")
                && item.get("call_id").and_then(Value::as_str) == Some(call_id.as_str())
        }) {
            input.push(serde_json::json!({
                "type": "function_call",
                "call_id": call_id,
                "name": name,
                "arguments": "{}",
            }));
        }
        input.push(serde_json::json!({
            "type": "function_call_output",
            "call_id": call_id,
            "output": output,
        }));
        return Ok(());
    }

    if role == "tool" {
        if !message.contains_key("content") {
            return Err(CompatError::invalid(
                format!("messages[{index}].content"),
                "Tool message `content` is required",
            ));
        }
        let call_id = message
            .get("tool_call_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                CompatError::invalid(
                    format!("messages[{index}].tool_call_id"),
                    "Tool messages require `tool_call_id`",
                )
            })?;
        let output = translate_tool_output(message.get("content"), index)?;
        let output_type = if custom_call_ids.contains(call_id) {
            "custom_tool_call_output"
        } else {
            "function_call_output"
        };
        input.push(serde_json::json!({
            "type": output_type,
            "call_id": call_id,
            "output": output,
        }));
        return Ok(());
    }
    if !matches!(role, "developer" | "system" | "user" | "assistant") {
        return Err(CompatError::invalid(
            format!("messages[{index}].role"),
            format!("Unsupported message role `{role}`"),
        ));
    }
    let participant_name = match message.get("name") {
        None | Some(Value::Null) => None,
        Some(Value::String(name)) => Some(name.as_str()),
        Some(_) => {
            return Err(CompatError::invalid(
                format!("messages[{index}].name"),
                "Message `name` must be a string",
            ));
        }
    };
    if role != "assistant" && message.get("content").is_none_or(Value::is_null) {
        return Err(CompatError::invalid(
            format!("messages[{index}].content"),
            "Message `content` is required",
        ));
    }
    if role == "assistant"
        && !["content", "function_call", "tool_calls", "refusal"]
            .iter()
            .any(|field| message.get(*field).is_some_and(|value| !value.is_null()))
    {
        return Err(CompatError::invalid(
            format!("messages[{index}].content"),
            "Assistant messages require `content`, `tool_calls`, or `refusal`",
        ));
    }

    let mut content = translate_message_content(message.get("content"), role, index)?;
    if let Some(name) = participant_name {
        content.insert(0, participant_name_content(role, name));
    }
    if let Some(refusal) = message.get("refusal").filter(|value| !value.is_null()) {
        if role != "assistant" {
            return Err(CompatError::invalid(
                format!("messages[{index}].refusal"),
                "Only assistant messages may contain `refusal`",
            ));
        }
        let refusal = refusal.as_str().ok_or_else(|| {
            CompatError::invalid(
                format!("messages[{index}].refusal"),
                "`refusal` must be a string or null",
            )
        })?;
        content.push(serde_json::json!({"type": "refusal", "refusal": refusal}));
    }
    if !content.is_empty() {
        input.push(serde_json::json!({
            "type": "message",
            "role": role,
            "content": content,
        }));
    }
    if message
        .get("function_call")
        .is_some_and(|value| !value.is_null())
        && message
            .get("tool_calls")
            .is_some_and(|value| !value.is_null())
    {
        return Err(CompatError::invalid(
            format!("messages[{index}].function_call"),
            "Assistant messages cannot contain both `function_call` and `tool_calls`",
        ));
    }
    if let Some(function_call) = message
        .get("function_call")
        .filter(|value| !value.is_null())
    {
        let path = format!("messages[{index}].function_call");
        let function_call = function_call
            .as_object()
            .ok_or_else(|| CompatError::invalid(&path, "`function_call` must be an object"))?;
        let name = required_string(function_call, "name", &path)?;
        let arguments = required_string(function_call, "arguments", &path)?;
        let call_id = synthetic_legacy_call_id(index, "call", explicit_call_ids);
        input.push(serde_json::json!({
            "type": "function_call",
            "call_id": call_id,
            "name": name,
            "arguments": arguments,
        }));
        legacy_calls
            .entry(name.to_string())
            .or_default()
            .push_back(call_id);
    }
    if let Some(tool_calls) = message.get("tool_calls") {
        if role != "assistant" {
            return Err(CompatError::invalid(
                format!("messages[{index}].tool_calls"),
                "Only assistant messages may contain `tool_calls`",
            ));
        }
        let tool_calls = tool_calls.as_array().ok_or_else(|| {
            CompatError::invalid(
                format!("messages[{index}].tool_calls"),
                "`tool_calls` must be an array",
            )
        })?;
        for (tool_index, tool_call) in tool_calls.iter().enumerate() {
            let translated = translate_tool_call(tool_call, index, tool_index)?;
            if translated.get("type").and_then(Value::as_str) == Some("custom_tool_call")
                && let Some(call_id) = translated.get("call_id").and_then(Value::as_str)
            {
                custom_call_ids.insert(call_id.to_string());
            }
            input.push(translated);
        }
    }
    Ok(())
}

fn translate_message_content(
    content: Option<&Value>,
    role: &str,
    message_index: usize,
) -> Result<Vec<Value>, CompatError> {
    let Some(content) = content else {
        return Ok(Vec::new());
    };
    match content {
        Value::Null => Ok(Vec::new()),
        Value::String(text) => Ok(vec![text_content(role, text, None)]),
        Value::Array(parts) => parts
            .iter()
            .enumerate()
            .map(|(part_index, part)| {
                let object = part.as_object().ok_or_else(|| {
                    CompatError::invalid(
                        format!("messages[{message_index}].content[{part_index}]"),
                        "Message content parts must be objects",
                    )
                })?;
                match object.get("type").and_then(Value::as_str) {
                    Some("text") => {
                        if role == "assistant"
                            && object
                                .get("prompt_cache_breakpoint")
                                .is_some_and(|value| !value.is_null())
                        {
                            return Err(CompatError::unsupported(format!(
                                "messages[{message_index}].content[{part_index}].prompt_cache_breakpoint"
                            )));
                        }
                        let text = object.get("text").and_then(Value::as_str).ok_or_else(|| {
                            CompatError::invalid(
                                format!(
                                    "messages[{message_index}].content[{part_index}].text"
                                ),
                                "Text content requires `text`",
                            )
                        })?;
                        Ok(text_content(role, text, Some(object)))
                    }
                    Some("image_url") => {
                        if role != "user" {
                            return Err(CompatError::unsupported(format!(
                                "messages[{message_index}].content[{part_index}]"
                            )));
                        }
                        let image = object.get("image_url").ok_or_else(|| {
                            CompatError::invalid(
                                format!(
                                    "messages[{message_index}].content[{part_index}].image_url"
                                ),
                                "Image content requires `image_url`",
                            )
                        })?;
                        let (url, detail) = match image {
                            Value::Object(image) => (
                                image.get("url").and_then(Value::as_str).ok_or_else(|| {
                                    CompatError::invalid(
                                        format!(
                                            "messages[{message_index}].content[{part_index}].image_url.url"
                                        ),
                                        "Image content requires a URL",
                                    )
                                })?,
                                image
                                    .get("detail")
                                    .filter(|value| !value.is_null())
                                    .cloned()
                                    .unwrap_or_else(|| Value::String("auto".to_string())),
                            ),
                            _ => {
                                return Err(CompatError::invalid(
                                    format!(
                                        "messages[{message_index}].content[{part_index}].image_url"
                                    ),
                                    "`image_url` must be an object",
                                ));
                            }
                        };
                        let mut translated = serde_json::json!({
                            "type": "input_image",
                            "image_url": url,
                            "detail": detail,
                        });
                        copy_prompt_cache_breakpoint(object, &mut translated);
                        Ok(translated)
                    }
                    Some("input_audio") => {
                        if role != "user" {
                            return Err(CompatError::unsupported(format!(
                                "messages[{message_index}].content[{part_index}]"
                            )));
                        }
                        translate_audio_content(object, message_index, part_index)
                    }
                    Some("file") => {
                        if role != "user" {
                            return Err(CompatError::unsupported(format!(
                                "messages[{message_index}].content[{part_index}]"
                            )));
                        }
                        translate_file_content(object, message_index, part_index)
                    }
                    Some("refusal") if role == "assistant" => {
                        let refusal =
                            object.get("refusal").and_then(Value::as_str).ok_or_else(|| {
                                CompatError::invalid(
                                    format!(
                                        "messages[{message_index}].content[{part_index}].refusal"
                                    ),
                                    "Refusal content requires `refusal`",
                                )
                            })?;
                        Ok(serde_json::json!({"type": "refusal", "refusal": refusal}))
                    }
                    Some(kind) => Err(CompatError::unsupported(format!(
                        "messages[{message_index}].content[{part_index}].type={kind}"
                    ))),
                    None => Err(CompatError::invalid(
                        format!("messages[{message_index}].content[{part_index}].type"),
                        "Message content parts require `type`",
                    )),
                }
            })
            .collect(),
        _ => Err(CompatError::invalid(
            format!("messages[{message_index}].content"),
            "Message content must be a string, array, or null",
        )),
    }
}

fn translate_audio_content(
    part: &Map<String, Value>,
    message_index: usize,
    part_index: usize,
) -> Result<Value, CompatError> {
    let path = format!("messages[{message_index}].content[{part_index}]");
    let audio = part
        .get("input_audio")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            CompatError::invalid(
                format!("{path}.input_audio"),
                "Audio content requires an `input_audio` object",
            )
        })?;
    for field in audio.keys() {
        if !matches!(field.as_str(), "data" | "format") {
            return Err(CompatError::invalid(
                format!("{path}.input_audio.{field}"),
                format!("Unknown parameter `{path}.input_audio.{field}`"),
            ));
        }
    }
    let data = required_string(audio, "data", &format!("{path}.input_audio"))?;
    let mime = match required_string(audio, "format", &format!("{path}.input_audio"))? {
        "wav" => "audio/wav",
        "mp3" => "audio/mpeg",
        format => {
            return Err(CompatError::invalid(
                format!("{path}.input_audio.format"),
                format!("Unsupported input audio format `{format}`"),
            ));
        }
    };
    let mut translated = serde_json::json!({
        "type": "input_audio",
        "audio_url": format!("data:{mime};base64,{data}"),
    });
    copy_prompt_cache_breakpoint(part, &mut translated);
    Ok(translated)
}

fn translate_file_content(
    part: &Map<String, Value>,
    message_index: usize,
    part_index: usize,
) -> Result<Value, CompatError> {
    let path = format!("messages[{message_index}].content[{part_index}]");
    let file = part.get("file").and_then(Value::as_object).ok_or_else(|| {
        CompatError::invalid(
            format!("{path}.file"),
            "File content requires a `file` object",
        )
    })?;
    let mut translated =
        Map::from_iter([("type".to_string(), Value::String("input_file".to_string()))]);
    for field in ["file_data", "file_id", "filename"] {
        let Some(value) = file.get(field).filter(|value| !value.is_null()) else {
            continue;
        };
        if !value.is_string() {
            return Err(CompatError::invalid(
                format!("{path}.file.{field}"),
                format!("`{field}` must be a string"),
            ));
        }
        translated.insert(field.to_string(), value.clone());
    }
    if !translated.contains_key("file_id") && !translated.contains_key("file_data") {
        return Err(CompatError::invalid(
            format!("{path}.file"),
            "File content requires `file_id` or `file_data`",
        ));
    }
    if let Some(breakpoint) = part
        .get("prompt_cache_breakpoint")
        .filter(|value| !value.is_null())
    {
        translated.insert("prompt_cache_breakpoint".to_string(), breakpoint.clone());
    }
    Ok(Value::Object(translated))
}

fn text_content(role: &str, text: &str, source: Option<&Map<String, Value>>) -> Value {
    let mut translated = if role == "assistant" {
        serde_json::json!({"type": "output_text", "text": text})
    } else {
        serde_json::json!({"type": "input_text", "text": text})
    };
    if let Some(source) = source {
        copy_prompt_cache_breakpoint(source, &mut translated);
    }
    translated
}

fn participant_name_content(role: &str, name: &str) -> Value {
    // Responses messages have no `name` field. Keep Chat's participant identity model-visible
    // without passing an unsupported property to the upstream API. JSON quoting makes the marker
    // unambiguous even when a name contains whitespace, quotes, or line breaks.
    let quoted_name = Value::String(name.to_string()).to_string();
    text_content(role, &format!("[participant name={quoted_name}]\n"), None)
}

fn copy_prompt_cache_breakpoint(source: &Map<String, Value>, destination: &mut Value) {
    if let Some(breakpoint) = source
        .get("prompt_cache_breakpoint")
        .filter(|value| !value.is_null())
    {
        destination["prompt_cache_breakpoint"] = breakpoint.clone();
    }
}

fn translate_tool_output(
    content: Option<&Value>,
    message_index: usize,
) -> Result<Value, CompatError> {
    match content {
        Some(Value::String(text)) => Ok(Value::String(text.clone())),
        Some(Value::Array(parts)) => {
            let mut output = Vec::with_capacity(parts.len());
            for (part_index, part) in parts.iter().enumerate() {
                let part = part.as_object().ok_or_else(|| {
                    CompatError::invalid(
                        format!("messages[{message_index}].content[{part_index}]"),
                        "Tool output content parts must be objects",
                    )
                })?;
                if part.get("type").and_then(Value::as_str) != Some("text") {
                    return Err(CompatError::unsupported(format!(
                        "messages[{message_index}].content[{part_index}]"
                    )));
                }
                let text = part.get("text").and_then(Value::as_str).ok_or_else(|| {
                    CompatError::invalid(
                        format!("messages[{message_index}].content[{part_index}].text"),
                        "Tool output text parts require `text`",
                    )
                })?;
                let mut translated = serde_json::json!({"type": "input_text", "text": text});
                copy_prompt_cache_breakpoint(part, &mut translated);
                output.push(translated);
            }
            Ok(Value::Array(output))
        }
        Some(Value::Null) | None => Err(CompatError::invalid(
            format!("messages[{message_index}].content"),
            "Tool output must be a string or text-part array",
        )),
        Some(_) => Err(CompatError::invalid(
            format!("messages[{message_index}].content"),
            "Tool output must be a string or text-part array",
        )),
    }
}

fn translate_tool_call(
    value: &Value,
    message_index: usize,
    tool_index: usize,
) -> Result<Value, CompatError> {
    let path = format!("messages[{message_index}].tool_calls[{tool_index}]");
    let call = value
        .as_object()
        .ok_or_else(|| CompatError::invalid(&path, "Tool calls must be objects"))?;
    let call_id = call
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| CompatError::invalid(format!("{path}.id"), "Tool calls require `id`"))?;
    match call.get("type").and_then(Value::as_str) {
        Some("function") => {
            let function = call
                .get("function")
                .and_then(Value::as_object)
                .ok_or_else(|| {
                    CompatError::invalid(
                        format!("{path}.function"),
                        "Function tool calls require `function`",
                    )
                })?;
            let name = required_string(function, "name", &format!("{path}.function"))?;
            let arguments = required_string(function, "arguments", &format!("{path}.function"))?;
            Ok(serde_json::json!({
                "type": "function_call",
                "call_id": call_id,
                "name": name,
                "arguments": arguments,
            }))
        }
        Some("custom") => {
            let custom = call
                .get("custom")
                .and_then(Value::as_object)
                .ok_or_else(|| {
                    CompatError::invalid(
                        format!("{path}.custom"),
                        "Custom tool calls require `custom`",
                    )
                })?;
            let name = required_string(custom, "name", &format!("{path}.custom"))?;
            let custom_input = required_string(custom, "input", &format!("{path}.custom"))?;
            Ok(serde_json::json!({
                "type": "custom_tool_call",
                "call_id": call_id,
                "name": name,
                "input": custom_input,
            }))
        }
        Some(kind) => Err(CompatError::unsupported(format!("{path}.type={kind}"))),
        None => Err(CompatError::invalid(
            format!("{path}.type"),
            "Tool calls require `type`",
        )),
    }
}

fn required_string<'a>(
    object: &'a Map<String, Value>,
    field: &str,
    path: &str,
) -> Result<&'a str, CompatError> {
    object.get(field).and_then(Value::as_str).ok_or_else(|| {
        CompatError::invalid(
            format!("{path}.{field}"),
            format!("`{path}.{field}` must be a string"),
        )
    })
}

fn translate_tools(
    request: &Map<String, Value>,
    responses: &mut Map<String, Value>,
) -> Result<(), CompatError> {
    let mut translated = Vec::new();
    if let Some(value) = request.get("tools") {
        let tools = value
            .as_array()
            .ok_or_else(|| CompatError::invalid("tools", "`tools` must be an array"))?;
        for (index, tool) in tools.iter().enumerate() {
            translated.push(translate_tool_definition(tool, &format!("tools[{index}]"))?);
        }
    }
    if let Some(value) = request.get("functions") {
        let functions = value
            .as_array()
            .ok_or_else(|| CompatError::invalid("functions", "`functions` must be an array"))?;
        for (index, function) in functions.iter().enumerate() {
            translated.push(translate_tool_definition(
                &serde_json::json!({"type": "function", "function": function}),
                &format!("functions[{index}]"),
            )?);
        }
    }
    if let Some(options) = request
        .get("web_search_options")
        .filter(|value| !value.is_null())
    {
        translated.push(translate_web_search_options(options)?);
    }
    if request.contains_key("tools")
        || request.contains_key("functions")
        || request
            .get("web_search_options")
            .is_some_and(|value| !value.is_null())
    {
        responses.insert("tools".to_string(), Value::Array(translated));
    }
    Ok(())
}

fn translate_web_search_options(value: &Value) -> Result<Value, CompatError> {
    let options = value.as_object().ok_or_else(|| {
        CompatError::invalid(
            "web_search_options",
            "`web_search_options` must be an object",
        )
    })?;
    for field in options.keys() {
        if !matches!(field.as_str(), "search_context_size" | "user_location") {
            return Err(CompatError::invalid(
                format!("web_search_options.{field}"),
                format!("Unknown parameter `web_search_options.{field}`"),
            ));
        }
    }
    let mut translated =
        Map::from_iter([("type".to_string(), Value::String("web_search".to_string()))]);
    if let Some(size) = options
        .get("search_context_size")
        .filter(|value| !value.is_null())
    {
        match size.as_str() {
            Some("low" | "medium" | "high") => {
                translated.insert("search_context_size".to_string(), size.clone());
            }
            _ => {
                return Err(CompatError::invalid(
                    "web_search_options.search_context_size",
                    "`search_context_size` must be `low`, `medium`, or `high`",
                ));
            }
        }
    }
    if let Some(location) = options
        .get("user_location")
        .filter(|value| !value.is_null())
    {
        let location = location.as_object().ok_or_else(|| {
            CompatError::invalid(
                "web_search_options.user_location",
                "`user_location` must be an object or null",
            )
        })?;
        for field in location.keys() {
            if !matches!(field.as_str(), "approximate" | "type") {
                return Err(CompatError::invalid(
                    format!("web_search_options.user_location.{field}"),
                    format!("Unknown parameter `web_search_options.user_location.{field}`"),
                ));
            }
        }
        if location.get("type").and_then(Value::as_str) != Some("approximate") {
            return Err(CompatError::invalid(
                "web_search_options.user_location.type",
                "`user_location.type` must be `approximate`",
            ));
        }
        let approximate = location
            .get("approximate")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                CompatError::invalid(
                    "web_search_options.user_location.approximate",
                    "`user_location.approximate` must be an object",
                )
            })?;
        let mut output_location =
            Map::from_iter([("type".to_string(), Value::String("approximate".to_string()))]);
        for (field, value) in approximate {
            if !matches!(field.as_str(), "city" | "country" | "region" | "timezone") {
                return Err(CompatError::invalid(
                    format!("web_search_options.user_location.approximate.{field}"),
                    format!(
                        "Unknown parameter `web_search_options.user_location.approximate.{field}`"
                    ),
                ));
            }
            if !value.is_string() {
                return Err(CompatError::invalid(
                    format!("web_search_options.user_location.approximate.{field}"),
                    format!("`{field}` must be a string"),
                ));
            }
            output_location.insert(field.clone(), value.clone());
        }
        translated.insert("user_location".to_string(), Value::Object(output_location));
    }
    Ok(Value::Object(translated))
}

fn translate_tool_definition(tool: &Value, path: &str) -> Result<Value, CompatError> {
    let tool = tool
        .as_object()
        .ok_or_else(|| CompatError::invalid(path, "Tools must be objects"))?;
    match tool.get("type").and_then(Value::as_str) {
        Some("function") => {
            let function = tool
                .get("function")
                .and_then(Value::as_object)
                .ok_or_else(|| {
                    CompatError::invalid(
                        format!("{path}.function"),
                        "Function tools require `function`",
                    )
                })?;
            required_string(function, "name", &format!("{path}.function"))?;
            let mut output = function.clone();
            output.insert("type".to_string(), Value::String("function".to_string()));
            Ok(Value::Object(output))
        }
        Some("custom") => {
            let custom = tool
                .get("custom")
                .and_then(Value::as_object)
                .ok_or_else(|| {
                    CompatError::invalid(format!("{path}.custom"), "Custom tools require `custom`")
                })?;
            let mut output = custom.clone();
            if let Some(format) = output.get_mut("format").and_then(Value::as_object_mut)
                && format.get("type").and_then(Value::as_str) == Some("grammar")
            {
                let grammar = format
                    .remove("grammar")
                    .and_then(|value| value.as_object().cloned())
                    .ok_or_else(|| {
                        CompatError::invalid(
                            format!("{path}.custom.format.grammar"),
                            "Grammar formats require `grammar`",
                        )
                    })?;
                format.extend(grammar);
            }
            output.insert("type".to_string(), Value::String("custom".to_string()));
            Ok(Value::Object(output))
        }
        Some(kind) => Err(CompatError::unsupported(format!("{path}.type={kind}"))),
        None => Err(CompatError::invalid(
            format!("{path}.type"),
            "Tools require `type`",
        )),
    }
}

fn legacy_function_names(request: &Map<String, Value>) -> HashSet<String> {
    let mut legacy = request
        .get("functions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|function| function.get("name").and_then(Value::as_str))
        .map(str::to_string)
        .collect::<HashSet<_>>();
    if let Some(name) = request
        .get("function_call")
        .and_then(Value::as_object)
        .and_then(|choice| choice.get("name"))
        .and_then(Value::as_str)
    {
        legacy.insert(name.to_string());
    }
    let modern = request
        .get("tools")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|tool| tool.get("type").and_then(Value::as_str) == Some("function"))
        .filter_map(|tool| tool.get("function"))
        .filter_map(|function| function.get("name").and_then(Value::as_str))
        .collect::<HashSet<_>>();
    legacy.retain(|name| !modern.contains(name.as_str()));
    legacy
}

fn translate_tool_choice(
    request: &Map<String, Value>,
    responses: &mut Map<String, Value>,
) -> Result<(), CompatError> {
    if request.contains_key("tool_choice") && request.contains_key("function_call") {
        return Err(CompatError::invalid(
            "tool_choice",
            "`tool_choice` and deprecated `function_call` cannot be used together",
        ));
    }
    let choice = request
        .get("tool_choice")
        .or_else(|| request.get("function_call"));
    let Some(choice) = choice else {
        return Ok(());
    };
    let translated = match choice {
        Value::String(_) => choice.clone(),
        Value::Object(choice) => {
            let kind = choice
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("function");
            if kind == "allowed_tools" {
                let allowed = choice
                    .get("allowed_tools")
                    .and_then(Value::as_object)
                    .ok_or_else(|| {
                        CompatError::invalid(
                            "tool_choice.allowed_tools",
                            "Allowed tool choice requires `allowed_tools`",
                        )
                    })?;
                let mode = required_string(allowed, "mode", "tool_choice.allowed_tools")?;
                let tools = allowed
                    .get("tools")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        CompatError::invalid(
                            "tool_choice.allowed_tools.tools",
                            "Allowed tool choice requires a `tools` array",
                        )
                    })?
                    .iter()
                    .enumerate()
                    .map(|(index, tool)| {
                        translate_tool_definition(
                            tool,
                            &format!("tool_choice.allowed_tools.tools[{index}]"),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                serde_json::json!({"type": "allowed_tools", "mode": mode, "tools": tools})
            } else if kind == "function" || kind == "custom" {
                let field = kind;
                let selected = choice
                    .get(field)
                    .and_then(Value::as_object)
                    .unwrap_or(choice);
                let name = required_string(selected, "name", &format!("tool_choice.{field}"))?;
                serde_json::json!({"type": kind, "name": name})
            } else {
                return Err(CompatError::unsupported("tool_choice.type"));
            }
        }
        _ => {
            return Err(CompatError::invalid(
                "tool_choice",
                "`tool_choice` must be a string or object",
            ));
        }
    };
    responses.insert("tool_choice".to_string(), translated);
    Ok(())
}

fn translate_text_config(
    request: &Map<String, Value>,
    responses: &mut Map<String, Value>,
) -> Result<(), CompatError> {
    let mut text = Map::new();
    if let Some(verbosity) = request.get("verbosity").filter(|value| !value.is_null()) {
        text.insert("verbosity".to_string(), verbosity.clone());
    }
    if let Some(format) = request
        .get("response_format")
        .filter(|value| !value.is_null())
    {
        let format = format.as_object().ok_or_else(|| {
            CompatError::invalid("response_format", "`response_format` must be an object")
        })?;
        let translated = match format.get("type").and_then(Value::as_str) {
            Some("text" | "json_object") => Value::Object(format.clone()),
            Some("json_schema") => {
                let schema = format
                    .get("json_schema")
                    .and_then(Value::as_object)
                    .ok_or_else(|| {
                        CompatError::invalid(
                            "response_format.json_schema",
                            "JSON schema format requires `json_schema`",
                        )
                    })?;
                let mut flattened = schema.clone();
                flattened.insert("type".to_string(), Value::String("json_schema".to_string()));
                Value::Object(flattened)
            }
            Some(kind) => return Err(CompatError::unsupported(format!("response_format={kind}"))),
            None => {
                return Err(CompatError::invalid(
                    "response_format.type",
                    "Response format requires `type`",
                ));
            }
        };
        text.insert("format".to_string(), translated);
    }
    if !text.is_empty() {
        responses.insert("text".to_string(), Value::Object(text));
    }
    Ok(())
}

fn streaming_response(
    upstream: reqwest::Response,
    request_model: String,
    include_usage: bool,
    logprobs_requested: bool,
    legacy_function_names: HashSet<String>,
    exchange_dump: Option<ExchangeDump>,
) -> Response {
    let headers = translated_headers(upstream.headers(), "text/event-stream");
    let mut events = Box::pin(upstream.bytes_stream().eventsource());
    let stream = async_stream::try_stream! {
        let mut translator = StreamTranslator::new(
            request_model,
            include_usage,
            logprobs_requested,
            legacy_function_names,
        );
        while let Some(event) = events.next().await {
            let event = event.map_err(|err| io::Error::other(err.to_string()))?;
            if event.data == "[DONE]" {
                break;
            }
            let event: Value = serde_json::from_str(&event.data)
                .map_err(|err| io::Error::other(format!("invalid Responses SSE event: {err}")))?;
            for frame in translator.translate_event(&event) {
                yield frame;
            }
            if translator.done {
                break;
            }
        }
        if !translator.done {
            yield sse_error("Responses stream closed before a terminal event");
            yield Bytes::from_static(b"data: [DONE]\n\n");
        }
    };
    stream_response(StatusCode::OK, headers, Box::pin(stream), exchange_dump)
}

type CompatStream = Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send>>;

fn stream_response(
    status: StatusCode,
    headers: HeaderMap,
    stream: CompatStream,
    exchange_dump: Option<ExchangeDump>,
) -> Response {
    let body = match exchange_dump {
        Some(exchange_dump) => {
            Body::from_stream(exchange_dump.tee_response_stream(status.as_u16(), &headers, stream))
        }
        None => Body::from_stream(stream),
    };
    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

async fn collect_completion(
    upstream: reqwest::Response,
    request_model: &str,
    logprobs_requested: bool,
    legacy_function_names: &HashSet<String>,
) -> Result<(HeaderMap, Bytes), CompatError> {
    let headers = translated_headers(upstream.headers(), "application/json");
    let mut events = Box::pin(upstream.bytes_stream().eventsource());
    let mut terminal_response = None;
    while let Some(event) = events.next().await {
        let event = event.map_err(|err| {
            CompatError::invalid("upstream", format!("Invalid Responses SSE stream: {err}"))
        })?;
        if event.data == "[DONE]" {
            break;
        }
        let event: Value = serde_json::from_str(&event.data).map_err(|err| {
            CompatError::invalid("upstream", format!("Invalid Responses SSE event: {err}"))
        })?;
        match event.get("type").and_then(Value::as_str) {
            Some("response.completed" | "response.incomplete") => {
                terminal_response = event.get("response").cloned();
                break;
            }
            Some("response.failed") => {
                return Err(CompatError::invalid(
                    "upstream",
                    response_error_message(&event),
                ));
            }
            Some("error") => {
                return Err(CompatError::invalid(
                    "upstream",
                    event
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("Responses API stream error"),
                ));
            }
            _ => {}
        }
    }
    let response = terminal_response.ok_or_else(|| {
        CompatError::invalid(
            "upstream",
            "Responses stream ended without a terminal response",
        )
    })?;
    let completion = completion_from_response(
        &response,
        request_model,
        logprobs_requested,
        legacy_function_names,
    );
    let body = serde_json::to_vec(&completion)
        .map(Bytes::from)
        .map_err(|err| CompatError::invalid("upstream", err.to_string()))?;
    Ok((headers, body))
}

fn buffered_response(
    headers: HeaderMap,
    body: Bytes,
    exchange_dump: Option<ExchangeDump>,
) -> Response {
    let stream: CompatStream = Box::pin(futures::stream::once(async move {
        Ok::<Bytes, io::Error>(body)
    }));
    stream_response(StatusCode::OK, headers, stream, exchange_dump)
}

#[derive(Clone, Copy)]
enum StreamCall {
    Tool(usize),
    LegacyFunction,
}

struct StreamTranslator {
    id: String,
    created: i64,
    model: String,
    service_tier: Option<Value>,
    system_fingerprint: Option<Value>,
    obfuscation: Option<String>,
    role_sent: bool,
    calls: HashMap<u64, StreamCall>,
    next_tool_index: usize,
    include_usage: bool,
    logprobs_requested: bool,
    legacy_function_names: HashSet<String>,
    has_legacy_function_call: bool,
    done: bool,
}

impl StreamTranslator {
    fn new(
        model: String,
        include_usage: bool,
        logprobs_requested: bool,
        legacy_function_names: HashSet<String>,
    ) -> Self {
        Self {
            id: "chatcmpl-pending".to_string(),
            created: unix_timestamp(),
            model,
            service_tier: None,
            system_fingerprint: None,
            obfuscation: None,
            role_sent: false,
            calls: HashMap::new(),
            next_tool_index: 0,
            include_usage,
            logprobs_requested,
            legacy_function_names,
            has_legacy_function_call: false,
            done: false,
        }
    }

    fn translate_event(&mut self, event: &Value) -> Vec<Bytes> {
        let mut frames = Vec::new();
        self.obfuscation = event
            .get("obfuscation")
            .and_then(Value::as_str)
            .map(str::to_string);
        match event.get("type").and_then(Value::as_str) {
            Some("response.created") => {
                if let Some(response) = event.get("response") {
                    self.update_metadata(response);
                }
                self.push_role(&mut frames);
            }
            Some("response.output_text.delta") => {
                self.push_role(&mut frames);
                if let Some(delta) = event.get("delta").and_then(Value::as_str) {
                    frames.push(self.text_chunk(delta, event.get("logprobs")));
                }
            }
            Some("response.refusal.delta") => {
                self.push_role(&mut frames);
                if let Some(delta) = event.get("delta").and_then(Value::as_str) {
                    frames.push(self.chunk(serde_json::json!({"refusal": delta}), None));
                }
            }
            Some("response.output_item.added") => {
                let item = event.get("item").unwrap_or(&Value::Null);
                let item_type = item.get("type").and_then(Value::as_str);
                if matches!(item_type, Some("function_call" | "custom_tool_call")) {
                    self.push_role(&mut frames);
                    let output_index = event
                        .get("output_index")
                        .and_then(Value::as_u64)
                        .unwrap_or(self.next_tool_index as u64);
                    let call_id = item
                        .get("call_id")
                        .or_else(|| item.get("id"))
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let name = item.get("name").and_then(Value::as_str).unwrap_or("");
                    if item_type == Some("function_call")
                        && self.legacy_function_names.contains(name)
                    {
                        self.calls.insert(output_index, StreamCall::LegacyFunction);
                        self.has_legacy_function_call = true;
                        frames.push(self.chunk(
                            serde_json::json!({
                                "function_call": {"name": name, "arguments": ""}
                            }),
                            None,
                        ));
                        return frames;
                    }
                    let tool_index = self.next_tool_index;
                    self.next_tool_index += 1;
                    self.calls
                        .insert(output_index, StreamCall::Tool(tool_index));
                    let delta = if item_type == Some("custom_tool_call") {
                        serde_json::json!({
                            "tool_calls": [{
                                "index": tool_index,
                                "id": call_id,
                                "type": "custom",
                                "custom": {"name": name, "input": ""},
                            }]
                        })
                    } else {
                        serde_json::json!({
                            "tool_calls": [{
                                "index": tool_index,
                                "id": call_id,
                                "type": "function",
                                "function": {"name": name, "arguments": ""},
                            }]
                        })
                    };
                    frames.push(self.chunk(delta, None));
                }
            }
            Some("response.function_call_arguments.delta") => {
                self.push_role(&mut frames);
                let output_index = event
                    .get("output_index")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                if let Some(delta) = event.get("delta").and_then(Value::as_str) {
                    match self.calls.get(&output_index).copied() {
                        Some(StreamCall::LegacyFunction) => frames.push(self.chunk(
                            serde_json::json!({"function_call": {"arguments": delta}}),
                            None,
                        )),
                        _ => {
                            let tool_index = match self.calls.get(&output_index).copied() {
                                Some(StreamCall::Tool(index)) => index,
                                _ => 0,
                            };
                            frames.push(self.chunk(
                                serde_json::json!({
                                    "tool_calls": [{
                                        "index": tool_index,
                                        "function": {"arguments": delta},
                                    }]
                                }),
                                None,
                            ));
                        }
                    }
                }
            }
            Some("response.custom_tool_call_input.delta") => {
                self.push_role(&mut frames);
                let output_index = event
                    .get("output_index")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let tool_index = match self.calls.get(&output_index).copied() {
                    Some(StreamCall::Tool(index)) => index,
                    _ => 0,
                };
                if let Some(delta) = event.get("delta").and_then(Value::as_str) {
                    frames.push(self.chunk(
                        serde_json::json!({
                            "tool_calls": [{
                                "index": tool_index,
                                "custom": {"input": delta},
                            }]
                        }),
                        None,
                    ));
                }
            }
            Some("response.completed" | "response.incomplete") => {
                let response = event.get("response").unwrap_or(&Value::Null);
                self.update_metadata(response);
                self.push_role(&mut frames);
                let finish_reason = if self.next_tool_index > 0 {
                    "tool_calls"
                } else if self.has_legacy_function_call {
                    "function_call"
                } else {
                    finish_reason(response, false)
                };
                frames.push(self.terminal_chunk(response, finish_reason));
                if self.include_usage {
                    frames.push(self.usage_chunk(response.get("usage")));
                }
                frames.push(Bytes::from_static(b"data: [DONE]\n\n"));
                self.done = true;
            }
            Some("response.failed") => {
                frames.push(sse_error(&response_error_message(event)));
                frames.push(Bytes::from_static(b"data: [DONE]\n\n"));
                self.done = true;
            }
            Some("error") => {
                frames.push(sse_error(
                    event
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("Responses API stream error"),
                ));
                frames.push(Bytes::from_static(b"data: [DONE]\n\n"));
                self.done = true;
            }
            _ => {}
        }
        frames
    }

    fn update_metadata(&mut self, response: &Value) {
        if let Some(id) = response.get("id").and_then(Value::as_str) {
            self.id = chat_completion_id(id);
        }
        if let Some(model) = response.get("model").and_then(Value::as_str) {
            self.model = model.to_string();
        }
        if let Some(service_tier) = response
            .get("service_tier")
            .filter(|value| !value.is_null())
        {
            self.service_tier = Some(service_tier.clone());
        }
        if let Some(system_fingerprint) = response
            .get("system_fingerprint")
            .filter(|value| !value.is_null())
        {
            self.system_fingerprint = Some(system_fingerprint.clone());
        }
        if let Some(created) = response.get("created_at").and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_f64().map(|value| value as i64))
        }) {
            self.created = created;
        }
    }

    fn push_role(&mut self, frames: &mut Vec<Bytes>) {
        if self.role_sent {
            return;
        }
        self.role_sent = true;
        frames.push(self.chunk(
            serde_json::json!({"role": "assistant", "content": ""}),
            None,
        ));
    }

    fn chunk(&self, delta: Value, finish_reason: Option<&str>) -> Bytes {
        sse_json(&self.chunk_value(delta, finish_reason))
    }

    fn text_chunk(&self, delta: &str, logprobs: Option<&Value>) -> Bytes {
        let mut chunk = self.chunk_value(serde_json::json!({"content": delta}), None);
        if self.logprobs_requested {
            chunk["choices"][0]["logprobs"] = chat_choice_logprobs(logprobs);
        }
        sse_json(&chunk)
    }

    fn chunk_value(&self, delta: Value, finish_reason: Option<&str>) -> Value {
        let mut chunk = serde_json::json!({
            "id": self.id,
            "object": "chat.completion.chunk",
            "created": self.created,
            "model": self.model,
            "choices": [{
                "index": 0,
                "delta": delta,
                "logprobs": null,
                "finish_reason": finish_reason,
            }],
        });
        if self.include_usage {
            chunk["usage"] = Value::Null;
        }
        if let Some(service_tier) = self.service_tier.as_ref() {
            chunk["service_tier"] = service_tier.clone();
        }
        if let Some(system_fingerprint) = self.system_fingerprint.as_ref() {
            chunk["system_fingerprint"] = system_fingerprint.clone();
        }
        if let Some(obfuscation) = self.obfuscation.as_ref() {
            chunk["obfuscation"] = Value::String(obfuscation.clone());
        }
        chunk
    }

    fn terminal_chunk(&self, response: &Value, finish_reason: &str) -> Bytes {
        let mut chunk = self.chunk_value(serde_json::json!({}), Some(finish_reason));
        if let Some(moderation) = response.get("moderation").filter(|value| !value.is_null()) {
            chunk["moderation"] = moderation.clone();
        }
        sse_json(&chunk)
    }

    fn usage_chunk(&self, usage: Option<&Value>) -> Bytes {
        let mut chunk = self.chunk_value(serde_json::json!({}), None);
        chunk["choices"] = Value::Array(Vec::new());
        chunk["usage"] = chat_usage(usage);
        sse_json(&chunk)
    }
}

fn completion_from_response(
    response: &Value,
    request_model: &str,
    logprobs_requested: bool,
    legacy_function_names: &HashSet<String>,
) -> Value {
    let mut content = String::new();
    let mut refusal = String::new();
    let mut annotations = Vec::new();
    let mut output_logprobs = Vec::new();
    let mut tool_calls = Vec::new();
    let mut legacy_function_call = None;
    if let Some(output) = response.get("output").and_then(Value::as_array) {
        for item in output {
            match item.get("type").and_then(Value::as_str) {
                Some("message") => {
                    if let Some(parts) = item.get("content").and_then(Value::as_array) {
                        for part in parts {
                            match part.get("type").and_then(Value::as_str) {
                                Some("output_text") => {
                                    let content_offset = content.chars().count();
                                    if let Some(text) = part.get("text").and_then(Value::as_str) {
                                        content.push_str(text);
                                    }
                                    if let Some(part_annotations) =
                                        part.get("annotations").and_then(Value::as_array)
                                    {
                                        annotations.extend(part_annotations.iter().filter_map(
                                            |annotation| {
                                                chat_annotation_with_offset(
                                                    annotation,
                                                    content_offset,
                                                )
                                            },
                                        ));
                                    }
                                    if logprobs_requested
                                        && let Some(logprobs) =
                                            part.get("logprobs").and_then(Value::as_array)
                                    {
                                        output_logprobs
                                            .extend(logprobs.iter().filter_map(chat_token_logprob));
                                    }
                                }
                                Some("refusal") => {
                                    if let Some(text) = part.get("refusal").and_then(Value::as_str)
                                    {
                                        refusal.push_str(text);
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
                Some("function_call") => {
                    let name = item.get("name").and_then(Value::as_str).unwrap_or("");
                    let arguments = item.get("arguments").and_then(Value::as_str).unwrap_or("");
                    if legacy_function_call.is_none() && legacy_function_names.contains(name) {
                        legacy_function_call = Some(serde_json::json!({
                            "name": name,
                            "arguments": arguments,
                        }));
                    } else {
                        tool_calls.push(serde_json::json!({
                            "id": item.get("call_id").or_else(|| item.get("id")).and_then(Value::as_str).unwrap_or(""),
                            "type": "function",
                            "function": {
                                "name": name,
                                "arguments": arguments,
                            },
                        }));
                    }
                }
                Some("custom_tool_call") => {
                    tool_calls.push(serde_json::json!({
                        "id": item.get("call_id").or_else(|| item.get("id")).and_then(Value::as_str).unwrap_or(""),
                        "type": "custom",
                        "custom": {
                            "name": item.get("name").and_then(Value::as_str).unwrap_or(""),
                            "input": item.get("input").and_then(Value::as_str).unwrap_or(""),
                        },
                    }));
                }
                _ => {}
            }
        }
    }
    let mut message = Map::new();
    message.insert("role".to_string(), Value::String("assistant".to_string()));
    message.insert(
        "content".to_string(),
        if content.is_empty()
            && (!tool_calls.is_empty() || legacy_function_call.is_some() || !refusal.is_empty())
        {
            Value::Null
        } else {
            Value::String(content)
        },
    );
    if !refusal.is_empty() {
        message.insert("refusal".to_string(), Value::String(refusal));
    }
    if !annotations.is_empty() {
        message.insert("annotations".to_string(), Value::Array(annotations));
    }
    if let Some(function_call) = legacy_function_call.as_ref() {
        message.insert("function_call".to_string(), function_call.clone());
    }
    if !tool_calls.is_empty() {
        message.insert("tool_calls".to_string(), Value::Array(tool_calls.clone()));
    }

    let mut completion = serde_json::json!({
        "id": chat_completion_id(response.get("id").and_then(Value::as_str).unwrap_or("pending")),
        "object": "chat.completion",
        "created": response
            .get("created_at")
            .and_then(|value| value.as_i64().or_else(|| value.as_f64().map(|value| value as i64)))
            .unwrap_or_else(unix_timestamp),
        "model": response.get("model").and_then(Value::as_str).unwrap_or(request_model),
        "choices": [{
            "index": 0,
            "message": Value::Object(message),
            "logprobs": if logprobs_requested {
                Value::Object(Map::from_iter([
                    ("content".to_string(), Value::Array(output_logprobs)),
                    ("refusal".to_string(), Value::Null),
                ]))
            } else {
                Value::Null
            },
            "finish_reason": if !tool_calls.is_empty() {
                "tool_calls"
            } else if legacy_function_call.is_some() {
                "function_call"
            } else {
                finish_reason(response, false)
            },
        }],
        "usage": chat_usage(response.get("usage")),
    });
    for field in [
        "metadata",
        "moderation",
        "service_tier",
        "system_fingerprint",
    ] {
        if let Some(value) = response.get(field).filter(|value| !value.is_null()) {
            completion[field] = value.clone();
        }
    }
    completion
}

fn chat_annotation_with_offset(annotation: &Value, offset: usize) -> Option<Value> {
    if annotation.get("type").and_then(Value::as_str) != Some("url_citation") {
        return None;
    }
    let start = annotation.get("start_index")?.as_u64()? + offset as u64;
    let end = annotation.get("end_index")?.as_u64()? + offset as u64;
    Some(serde_json::json!({
        "type": "url_citation",
        "url_citation": {
            "start_index": start,
            "end_index": end,
            "title": annotation.get("title")?.as_str()?,
            "url": annotation.get("url")?.as_str()?,
        }
    }))
}

fn chat_choice_logprobs(logprobs: Option<&Value>) -> Value {
    let content = logprobs
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(chat_token_logprob)
        .collect::<Vec<_>>();
    serde_json::json!({"content": content, "refusal": null})
}

fn chat_token_logprob(logprob: &Value) -> Option<Value> {
    let token = logprob.get("token")?.as_str()?;
    let probability = logprob.get("logprob")?.as_f64()?;
    let bytes = logprob
        .get("bytes")
        .filter(|value| value.is_array() || value.is_null())
        .cloned()
        .unwrap_or_else(|| serde_json::json!(token.as_bytes()));
    let top_logprobs = logprob
        .get("top_logprobs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|top| {
            let token = top.get("token")?.as_str()?;
            let probability = top.get("logprob")?.as_f64()?;
            let bytes = top
                .get("bytes")
                .filter(|value| value.is_array() || value.is_null())
                .cloned()
                .unwrap_or_else(|| serde_json::json!(token.as_bytes()));
            Some(serde_json::json!({
                "token": token,
                "bytes": bytes,
                "logprob": probability,
            }))
        })
        .collect::<Vec<_>>();
    Some(serde_json::json!({
        "token": token,
        "bytes": bytes,
        "logprob": probability,
        "top_logprobs": top_logprobs,
    }))
}

fn finish_reason(response: &Value, has_tool_calls: bool) -> &'static str {
    if has_tool_calls {
        return "tool_calls";
    }
    match response
        .get("incomplete_details")
        .and_then(|details| details.get("reason"))
        .and_then(Value::as_str)
    {
        Some("max_output_tokens") => "length",
        Some("content_filter") => "content_filter",
        _ => "stop",
    }
}

fn chat_usage(usage: Option<&Value>) -> Value {
    let usage = usage.unwrap_or(&Value::Null);
    let input_tokens = usage
        .get("input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output_tokens = usage
        .get("output_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let total_tokens = usage
        .get("total_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(input_tokens + output_tokens);
    let mut translated = serde_json::json!({
        "prompt_tokens": input_tokens,
        "completion_tokens": output_tokens,
        "total_tokens": total_tokens,
    });
    if let Some(details) = usage.get("input_tokens_details") {
        translated["prompt_tokens_details"] = details.clone();
    }
    if let Some(details) = usage.get("output_tokens_details") {
        translated["completion_tokens_details"] = details.clone();
    }
    translated
}

fn response_error_message(event: &Value) -> String {
    event
        .get("response")
        .and_then(|response| response.get("error"))
        .and_then(|error| error.get("message"))
        .or_else(|| event.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("Responses API request failed")
        .to_string()
}

fn chat_completion_id(response_id: &str) -> String {
    format!(
        "chatcmpl-{}",
        response_id.strip_prefix("resp_").unwrap_or(response_id)
    )
}

fn unix_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs() as i64)
}

fn sse_json(value: &Value) -> Bytes {
    Bytes::from(format!("data: {value}\n\n"))
}

fn sse_error(message: &str) -> Bytes {
    sse_json(&serde_json::json!({
        "error": {
            "message": message,
            "type": "proxy_error",
            "param": null,
            "code": "upstream_stream_failed",
        }
    }))
}

fn translated_headers(upstream: &HeaderMap, content_type: &'static str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in upstream {
        if !crate::chatgpt::is_filtered_response_header(name)
            && !matches!(
                name.as_str(),
                "content-encoding" | "content-length" | "content-type"
            )
        {
            headers.append(name, value.clone());
        }
    }
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static(content_type),
    );
    headers
}

#[derive(Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    message: String,
    r#type: &'static str,
    param: Option<String>,
    code: &'static str,
}

fn invalid_request_response(error: CompatError) -> Response {
    (
        StatusCode::BAD_REQUEST,
        axum::Json(ErrorEnvelope {
            error: ErrorBody {
                message: error.message,
                r#type: "invalid_request_error",
                param: error.param,
                code: error.code,
            },
        }),
    )
        .into_response()
}

fn proxy_error_response(status: StatusCode, message: String) -> Response {
    (
        status,
        axum::Json(ErrorEnvelope {
            error: ErrorBody {
                message,
                r#type: "proxy_error",
                param: None,
                code: "upstream_request_failed",
            },
        }),
    )
        .into_response()
}

#[cfg(test)]
#[path = "chat_completions_tests.rs"]
mod tests;
