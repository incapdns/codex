use std::collections::HashMap;
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
            exchange_dump,
        )
    } else {
        match collect_completion(upstream, &translated.model).await {
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

    if request.get("store").and_then(Value::as_bool) == Some(true) {
        return Err(CompatError::invalid(
            "store",
            "`store: true` is unavailable because the ChatGPT Responses backend requires `store: false`",
        ));
    }
    if let Some(n) = request.get("n").and_then(Value::as_u64)
        && n != 1
    {
        return Err(CompatError::invalid(
            "n",
            "Only `n: 1` is supported by the Responses API compatibility layer",
        ));
    }
    for unsupported in [
        "audio",
        "frequency_penalty",
        "logit_bias",
        "logprobs",
        "modalities",
        "prediction",
        "presence_penalty",
        "seed",
        "stop",
        "top_logprobs",
        "web_search_options",
    ] {
        if request
            .get(unsupported)
            .is_some_and(|value| !value.is_null())
        {
            return Err(CompatError::unsupported(unsupported));
        }
    }

    let mut input = Vec::new();
    for (index, message) in messages.iter().enumerate() {
        translate_message(message, index, &mut input)?;
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
        "prompt_cache_key",
        "safety_identifier",
        "service_tier",
        "temperature",
        "top_p",
        "user",
    ] {
        copy_if_present(request, &mut responses, field, field);
    }
    if let Some(max_tokens) = request
        .get("max_completion_tokens")
        .or_else(|| request.get("max_tokens"))
    {
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
    })
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

fn translate_message(
    value: &Value,
    index: usize,
    input: &mut Vec<Value>,
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
    if message.get("name").is_some_and(|value| !value.is_null()) {
        return Err(CompatError::unsupported(format!("messages[{index}].name")));
    }

    if role == "tool" {
        let call_id = message
            .get("tool_call_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                CompatError::invalid(
                    format!("messages[{index}].tool_call_id"),
                    "Tool messages require `tool_call_id`",
                )
            })?;
        let output = tool_output_text(message.get("content"), index)?;
        input.push(serde_json::json!({
            "type": "function_call_output",
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

    let content = translate_message_content(message.get("content"), role, index)?;
    if !content.is_empty() {
        input.push(serde_json::json!({
            "type": "message",
            "role": role,
            "content": content,
        }));
    }
    if role == "assistant"
        && let Some(refusal) = message.get("refusal").and_then(Value::as_str)
    {
        input.push(serde_json::json!({
            "type": "message",
            "role": "assistant",
            "content": [{"type": "refusal", "refusal": refusal}],
        }));
    }
    if let Some(tool_calls) = message.get("tool_calls") {
        let tool_calls = tool_calls.as_array().ok_or_else(|| {
            CompatError::invalid(
                format!("messages[{index}].tool_calls"),
                "`tool_calls` must be an array",
            )
        })?;
        for (tool_index, tool_call) in tool_calls.iter().enumerate() {
            input.push(translate_tool_call(tool_call, index, tool_index)?);
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
        Value::String(text) => Ok(vec![text_content(role, text)]),
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
                    Some("text" | "input_text" | "output_text") => {
                        let text = object.get("text").and_then(Value::as_str).ok_or_else(|| {
                            CompatError::invalid(
                                format!(
                                    "messages[{message_index}].content[{part_index}].text"
                                ),
                                "Text content requires `text`",
                            )
                        })?;
                        Ok(text_content(role, text))
                    }
                    Some("image_url") => {
                        if role == "assistant" {
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
                            Value::String(url) => (url.as_str(), None),
                            Value::Object(image) => (
                                image.get("url").and_then(Value::as_str).ok_or_else(|| {
                                    CompatError::invalid(
                                        format!(
                                            "messages[{message_index}].content[{part_index}].image_url.url"
                                        ),
                                        "Image content requires a URL",
                                    )
                                })?,
                                image.get("detail").cloned(),
                            ),
                            _ => {
                                return Err(CompatError::invalid(
                                    format!(
                                        "messages[{message_index}].content[{part_index}].image_url"
                                    ),
                                    "`image_url` must be a string or object",
                                ));
                            }
                        };
                        let mut translated = serde_json::json!({
                            "type": "input_image",
                            "image_url": url,
                        });
                        if let Some(detail) = detail {
                            translated["detail"] = detail;
                        }
                        Ok(translated)
                    }
                    Some("file") => {
                        if role == "assistant" {
                            return Err(CompatError::unsupported(format!(
                                "messages[{message_index}].content[{part_index}]"
                            )));
                        }
                        translate_file_content(object, message_index, part_index)
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

fn text_content(role: &str, text: &str) -> Value {
    if role == "assistant" {
        serde_json::json!({"type": "output_text", "text": text})
    } else {
        serde_json::json!({"type": "input_text", "text": text})
    }
}

fn tool_output_text(content: Option<&Value>, message_index: usize) -> Result<String, CompatError> {
    match content {
        Some(Value::String(text)) => Ok(text.clone()),
        Some(Value::Array(parts)) => {
            let mut output = String::new();
            for (part_index, part) in parts.iter().enumerate() {
                let text = part
                    .as_object()
                    .filter(|part| {
                        matches!(
                            part.get("type").and_then(Value::as_str),
                            Some("text" | "input_text" | "output_text")
                        )
                    })
                    .and_then(|part| part.get("text"))
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        CompatError::unsupported(format!(
                            "messages[{message_index}].content[{part_index}]"
                        ))
                    })?;
                output.push_str(text);
            }
            Ok(output)
        }
        Some(Value::Null) | None => Ok(String::new()),
        Some(_) => Err(CompatError::invalid(
            format!("messages[{message_index}].content"),
            "Tool output must be a string, text-part array, or null",
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
    if call.get("type").and_then(Value::as_str) != Some("function") {
        return Err(CompatError::unsupported(format!("{path}.type")));
    }
    let call_id = call
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| CompatError::invalid(format!("{path}.id"), "Tool calls require `id`"))?;
    let function = call
        .get("function")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            CompatError::invalid(
                format!("{path}.function"),
                "Function tool calls require `function`",
            )
        })?;
    let name = function
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            CompatError::invalid(
                format!("{path}.function.name"),
                "Function tool calls require `name`",
            )
        })?;
    let arguments = function
        .get("arguments")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            CompatError::invalid(
                format!("{path}.function.arguments"),
                "Function tool calls require string `arguments`",
            )
        })?;
    Ok(serde_json::json!({
        "type": "function_call",
        "call_id": call_id,
        "name": name,
        "arguments": arguments,
    }))
}

fn translate_tools(
    request: &Map<String, Value>,
    responses: &mut Map<String, Value>,
) -> Result<(), CompatError> {
    let tools = if let Some(tools) = request.get("tools") {
        tools
            .as_array()
            .cloned()
            .ok_or_else(|| CompatError::invalid("tools", "`tools` must be an array"))?
    } else if let Some(functions) = request.get("functions") {
        functions
            .as_array()
            .ok_or_else(|| CompatError::invalid("functions", "`functions` must be an array"))?
            .iter()
            .map(|function| serde_json::json!({"type": "function", "function": function}))
            .collect()
    } else {
        return Ok(());
    };

    let translated = tools
        .iter()
        .enumerate()
        .map(|(index, tool)| {
            let tool = tool.as_object().ok_or_else(|| {
                CompatError::invalid(format!("tools[{index}]"), "Tools must be objects")
            })?;
            match tool.get("type").and_then(Value::as_str) {
                Some("function") => {
                    let function =
                        tool.get("function")
                            .and_then(Value::as_object)
                            .ok_or_else(|| {
                                CompatError::invalid(
                                    format!("tools[{index}].function"),
                                    "Function tools require `function`",
                                )
                            })?;
                    let mut output = function.clone();
                    output.insert("type".to_string(), Value::String("function".to_string()));
                    Ok(Value::Object(output))
                }
                Some(kind) => Err(CompatError::unsupported(format!(
                    "tools[{index}].type={kind}"
                ))),
                None => Err(CompatError::invalid(
                    format!("tools[{index}].type"),
                    "Tools require `type`",
                )),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    responses.insert("tools".to_string(), Value::Array(translated));
    Ok(())
}

fn translate_tool_choice(
    request: &Map<String, Value>,
    responses: &mut Map<String, Value>,
) -> Result<(), CompatError> {
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
            if kind != "function" {
                return Err(CompatError::unsupported("tool_choice.type"));
            }
            let function = choice
                .get("function")
                .and_then(Value::as_object)
                .unwrap_or(choice);
            let name = function
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    CompatError::invalid("tool_choice.function.name", "Tool choice requires `name`")
                })?;
            serde_json::json!({"type": "function", "name": name})
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
    exchange_dump: Option<ExchangeDump>,
) -> Response {
    let headers = translated_headers(upstream.headers(), "text/event-stream");
    let mut events = Box::pin(upstream.bytes_stream().eventsource());
    let stream = async_stream::try_stream! {
        let mut translator = StreamTranslator::new(request_model, include_usage);
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
    let completion = completion_from_response(&response, request_model);
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

struct StreamTranslator {
    id: String,
    created: i64,
    model: String,
    role_sent: bool,
    tool_calls: HashMap<u64, usize>,
    next_tool_index: usize,
    include_usage: bool,
    done: bool,
}

impl StreamTranslator {
    fn new(model: String, include_usage: bool) -> Self {
        Self {
            id: "chatcmpl-pending".to_string(),
            created: unix_timestamp(),
            model,
            role_sent: false,
            tool_calls: HashMap::new(),
            next_tool_index: 0,
            include_usage,
            done: false,
        }
    }

    fn translate_event(&mut self, event: &Value) -> Vec<Bytes> {
        let mut frames = Vec::new();
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
                    frames.push(self.chunk(serde_json::json!({"content": delta}), None));
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
                if item.get("type").and_then(Value::as_str) == Some("function_call") {
                    self.push_role(&mut frames);
                    let output_index = event
                        .get("output_index")
                        .and_then(Value::as_u64)
                        .unwrap_or(self.next_tool_index as u64);
                    let tool_index = self.next_tool_index;
                    self.next_tool_index += 1;
                    self.tool_calls.insert(output_index, tool_index);
                    let call_id = item
                        .get("call_id")
                        .or_else(|| item.get("id"))
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let name = item.get("name").and_then(Value::as_str).unwrap_or("");
                    frames.push(self.chunk(
                        serde_json::json!({
                            "tool_calls": [{
                                "index": tool_index,
                                "id": call_id,
                                "type": "function",
                                "function": {"name": name, "arguments": ""},
                            }]
                        }),
                        None,
                    ));
                }
            }
            Some("response.function_call_arguments.delta") => {
                self.push_role(&mut frames);
                let output_index = event
                    .get("output_index")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let tool_index = self.tool_calls.get(&output_index).copied().unwrap_or(0);
                if let Some(delta) = event.get("delta").and_then(Value::as_str) {
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
            Some("response.completed" | "response.incomplete") => {
                let response = event.get("response").unwrap_or(&Value::Null);
                self.update_metadata(response);
                self.push_role(&mut frames);
                let finish_reason = finish_reason(response, !self.tool_calls.is_empty());
                frames.push(self.chunk(serde_json::json!({}), Some(finish_reason)));
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
        sse_json(&serde_json::json!({
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
        }))
    }

    fn usage_chunk(&self, usage: Option<&Value>) -> Bytes {
        sse_json(&serde_json::json!({
            "id": self.id,
            "object": "chat.completion.chunk",
            "created": self.created,
            "model": self.model,
            "choices": [],
            "usage": chat_usage(usage),
        }))
    }
}

fn completion_from_response(response: &Value, request_model: &str) -> Value {
    let mut content = String::new();
    let mut refusal = String::new();
    let mut tool_calls = Vec::new();
    if let Some(output) = response.get("output").and_then(Value::as_array) {
        for item in output {
            match item.get("type").and_then(Value::as_str) {
                Some("message") => {
                    if let Some(parts) = item.get("content").and_then(Value::as_array) {
                        for part in parts {
                            match part.get("type").and_then(Value::as_str) {
                                Some("output_text") => {
                                    if let Some(text) = part.get("text").and_then(Value::as_str) {
                                        content.push_str(text);
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
                    tool_calls.push(serde_json::json!({
                        "id": item.get("call_id").or_else(|| item.get("id")).and_then(Value::as_str).unwrap_or(""),
                        "type": "function",
                        "function": {
                            "name": item.get("name").and_then(Value::as_str).unwrap_or(""),
                            "arguments": item.get("arguments").and_then(Value::as_str).unwrap_or(""),
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
        if content.is_empty() && !tool_calls.is_empty() {
            Value::Null
        } else {
            Value::String(content)
        },
    );
    if !refusal.is_empty() {
        message.insert("refusal".to_string(), Value::String(refusal));
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
            "logprobs": null,
            "finish_reason": finish_reason(response, !tool_calls.is_empty()),
        }],
        "usage": chat_usage(response.get("usage")),
    });
    for field in ["service_tier", "system_fingerprint"] {
        if let Some(value) = response.get(field).filter(|value| !value.is_null()) {
            completion[field] = value.clone();
        }
    }
    completion
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
