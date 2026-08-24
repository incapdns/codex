use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::extract::DefaultBodyLimit;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderName;
use axum::http::Method;
use axum::http::StatusCode;
use axum::http::Uri;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use codex_core::config::ConfigBuilder;
use codex_http_client::ClientRouteClass;
use codex_http_client::HttpClient;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use codex_login::default_client::create_client_for_route_async;
use codex_model_provider::auth_provider_from_auth;
use serde::Serialize;
use serde_json::Map;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::Args;
use crate::DEFAULT_LISTEN_HOST;
use crate::conversations::ConversationStore;
use crate::dump::ExchangeDumper;
use crate::routes::ResponsesRoute;
use crate::routes::resolve_responses_route;
use crate::warn_if_non_loopback;
use crate::write_server_info;

#[derive(Clone)]
pub(crate) struct ChatgptState {
    pub(crate) auth_manager: Arc<AuthManager>,
    pub(crate) client: HttpClient,
    pub(crate) upstream_url: reqwest::Url,
    pub(crate) upstream_headers: HeaderMap,
    pub(crate) dump_dir: Option<Arc<ExchangeDumper>>,
    pub(crate) chat_completions_compat: bool,
    pub(crate) conversations: Option<Arc<ConversationStore>>,
    pub(crate) shutdown: CancellationToken,
}

pub(crate) fn run_main(args: Args) -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building ChatGPT proxy runtime")?
        .block_on(run(args))
}

async fn run(args: Args) -> Result<()> {
    let cli_overrides = args
        .config_overrides
        .parse_overrides()
        .map_err(|message| anyhow!(message))?;
    let config = ConfigBuilder::default()
        .cli_overrides(cli_overrides)
        .strict_config(args.strict_config)
        .build()
        .await
        .context("loading Codex configuration")?;
    let effective_config = config.config_layer_stack.effective_config();
    let proxy_config = effective_config
        .get("responses_api_proxy")
        .and_then(toml::Value::as_table);
    let listen_host = match args.listen_host {
        Some(listen_host) => listen_host,
        None => match proxy_config
            .and_then(|value| value.get("listen_host"))
            .and_then(toml::Value::as_str)
        {
            Some(listen_host) => listen_host.parse().with_context(|| {
                format!("parsing responses_api_proxy.listen_host `{listen_host}`")
            })?,
            None => DEFAULT_LISTEN_HOST,
        },
    };
    let chat_completions_compat = args.chat_completions_compat.unwrap_or_else(|| {
        proxy_config
            .and_then(|value| value.get("chat_completions_compat"))
            .and_then(toml::Value::as_bool)
            .unwrap_or(true)
    });
    let conversations_compat = args.conversations_compat.unwrap_or_else(|| {
        proxy_config
            .and_then(|value| value.get("conversations_compat"))
            .and_then(toml::Value::as_bool)
            .unwrap_or(true)
    });
    let conversation_store_path = args
        .conversation_store
        .clone()
        .or_else(|| {
            proxy_config
                .and_then(|value| value.get("conversation_store"))
                .and_then(toml::Value::as_str)
                .map(PathBuf::from)
        })
        .unwrap_or_else(|| {
            config
                .sqlite
                .home()
                .join(crate::conversations::DEFAULT_STORE_FILENAME)
        });
    let compact_after_items = args.conversation_compact_after_items.unwrap_or_else(|| {
        proxy_config
            .and_then(|value| value.get("conversation_compact_after_items"))
            .and_then(toml::Value::as_integer)
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(crate::conversations::DEFAULT_COMPACT_AFTER_ITEMS)
    });
    let conversations = if conversations_compat {
        Some(Arc::new(
            ConversationStore::load(conversation_store_path, compact_after_items)
                .await
                .context("loading local Conversations compatibility store")?,
        ))
    } else {
        None
    };
    let auth_manager =
        AuthManager::shared_from_config(&config, /*enable_codex_api_key_env*/ false)
            .await
            .context("initializing ChatGPT authentication")?;
    let auth = require_chatgpt_auth(auth_manager.auth().await)?;
    let provider = config
        .model_provider
        .to_api_provider(Some(auth.auth_mode()))
        .context("resolving the configured model provider")?;
    let upstream_url = args
        .upstream_url
        .clone()
        .unwrap_or_else(|| provider.url_for_path("responses"));
    let upstream_url = reqwest::Url::parse(&upstream_url)
        .context("parsing --upstream-url or the configured provider URL")?;
    crate::routes::validate_upstream_create_url(&upstream_url)
        .context("validating --upstream-url or the configured provider URL")?;
    let client = create_client_for_route_async(
        config.http_client_factory(),
        upstream_url.to_string(),
        ClientRouteClass::Api,
    )
    .await
    .context("building the Codex API HTTP client")?;
    let dump_dir = args
        .dump_dir
        .clone()
        .map(ExchangeDumper::new)
        .transpose()
        .context("creating --dump-dir")?
        .map(Arc::new);
    warn_if_non_loopback(listen_host);
    let listener = tokio::net::TcpListener::bind((listen_host, args.port.unwrap_or(0)))
        .await
        .context("binding ChatGPT Responses API proxy")?;
    let bound_addr = listener.local_addr().context("reading proxy address")?;
    if let Some(path) = args.server_info.as_ref() {
        write_server_info(path, bound_addr.port())?;
    }

    let shutdown = CancellationToken::new();
    let state = ChatgptState {
        auth_manager,
        client,
        upstream_url,
        upstream_headers: provider.headers,
        dump_dir,
        chat_completions_compat,
        conversations,
        shutdown: shutdown.clone(),
    };
    let router = Router::new().fallback(responses);
    let router = if args.http_shutdown {
        router.route("/shutdown", get(shutdown_server))
    } else {
        router
    };
    let router = router.layer(DefaultBodyLimit::disable()).with_state(state);

    eprintln!("responses-api-proxy listening on {bound_addr}");
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await
        .context("serving ChatGPT Responses API proxy")?;
    Ok(())
}

async fn responses(
    State(state): State<ChatgptState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request_uri = uri
        .path_and_query()
        .map_or_else(|| uri.path(), |path_and_query| path_and_query.as_str());
    if let Some(response) = crate::models::handle(&state, &method, request_uri, &headers).await {
        return response;
    }
    if uri.path().starts_with("/v1/conversations") {
        let Some(conversations) = state.conversations.as_ref() else {
            return StatusCode::FORBIDDEN.into_response();
        };
        let exchange_dump = state.dump_dir.as_deref().and_then(|dump_dir| {
            dump_dir
                .dump_http_request(&method, request_uri, &headers, &body)
                .map_err(|err| {
                    eprintln!("responses-api-proxy failed to dump request: {err}");
                    err
                })
                .ok()
        });
        let response = conversations
            .handle(&method, &uri, &body)
            .await
            .unwrap_or_else(|| StatusCode::FORBIDDEN.into_response());
        return tee_local_response(response, exchange_dump);
    }
    if method == Method::POST && request_uri == "/v1/chat/completions" {
        if !state.chat_completions_compat {
            return StatusCode::FORBIDDEN.into_response();
        }
        return crate::chat_completions::handle(state, headers, body).await;
    }
    let Some(route) = resolve_responses_route(method.as_str(), request_uri) else {
        return StatusCode::FORBIDDEN.into_response();
    };
    if let Err(error) = validate_response_request_collections(&route, &body) {
        return error.into_response();
    }

    let exchange_dump = state.dump_dir.as_deref().and_then(|dump_dir| {
        dump_dir
            .dump_http_request(&method, request_uri, &headers, &body)
            .map_err(|err| {
                eprintln!("responses-api-proxy failed to dump request: {err}");
                err
            })
            .ok()
    });
    let mut prepared = if route.is_create() {
        match state.conversations.as_ref() {
            Some(conversations) => match conversations.begin_response(&body).await {
                Ok(prepared) => prepared,
                Err(err) => return err.into_response(),
            },
            None if request_has_conversation(&body) => {
                return crate::conversations::ConversationError::invalid(
                    "conversation",
                    "Local Conversations compatibility is disabled",
                )
                .into_response();
            }
            None => None,
        }
    } else {
        None
    };
    if let (Some(conversations), Some(prepared)) = (state.conversations.as_ref(), prepared.as_mut())
    {
        conversations
            .maybe_compact(&state, &headers, prepared)
            .await;
    }
    let upstream_body = match (&state.conversations, prepared.as_ref()) {
        (Some(conversations), Some(prepared)) => match conversations.upstream_body(prepared) {
            Ok(body) => body,
            Err(err) => {
                conversations.abort_response(prepared).await;
                return err.into_response();
            }
        },
        _ => body,
    };
    match forward_request(&state, &route, headers, upstream_body).await {
        Ok(response) => match (state.conversations.clone(), prepared) {
            (Some(conversations), Some(prepared)) => {
                conversations
                    .adapt_upstream_response(response, prepared, exchange_dump)
                    .await
            }
            _ => upstream_response(response, exchange_dump),
        },
        Err(err) => {
            if let (Some(conversations), Some(prepared)) =
                (state.conversations.as_ref(), prepared.as_ref())
            {
                conversations.abort_response(prepared).await;
            }
            eprintln!("forwarding error: {err:#}");
            error_response(StatusCode::BAD_GATEWAY, err.to_string())
        }
    }
}

fn request_has_conversation(body: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|value| value.get("conversation").cloned())
        .is_some_and(|value| !value.is_null())
}

fn tee_local_response(
    response: Response,
    exchange_dump: Option<crate::dump::ExchangeDump>,
) -> Response {
    let Some(exchange_dump) = exchange_dump else {
        return response;
    };
    let (parts, body) = response.into_parts();
    let stream = exchange_dump.tee_response_stream(
        parts.status.as_u16(),
        &parts.headers,
        body.into_data_stream(),
    );
    Response::from_parts(parts, Body::from_stream(stream))
}

pub(crate) async fn forward_request(
    state: &ChatgptState,
    route: &ResponsesRoute,
    incoming_headers: HeaderMap,
    body: Bytes,
) -> Result<reqwest::Response> {
    let body = normalize_create_body(route, body);
    let upstream_url = route
        .upstream_url(&state.upstream_url)
        .context("constructing the Responses resource URL")?;
    authenticated_request(
        state,
        route.method.clone(),
        upstream_url,
        incoming_headers,
        body,
    )
    .await
}

pub(crate) async fn authenticated_request(
    state: &ChatgptState,
    method: Method,
    upstream_url: reqwest::Url,
    incoming_headers: HeaderMap,
    body: Bytes,
) -> Result<reqwest::Response> {
    let mut auth_recovery = state.auth_manager.unauthorized_recovery();
    loop {
        let auth = require_chatgpt_auth(state.auth_manager.auth().await)?;
        let auth_headers = auth_provider_from_auth(&auth)
            .resolve_auth_headers()
            .await
            .context("resolving ChatGPT request headers")?;
        let mut headers = state.upstream_headers.clone();
        extend_forwarded_headers(&mut headers, &incoming_headers);
        headers.extend(auth_headers);

        let response = state
            .client
            .request(method.clone(), upstream_url.clone())
            .headers(headers)
            .body(body.clone())
            .send()
            .await
            .context("sending authenticated request to the Codex backend")?;
        if response.status() != StatusCode::UNAUTHORIZED || !auth_recovery.has_next() {
            return Ok(response);
        }
        if let Err(err) = auth_recovery.next().await {
            eprintln!("ChatGPT authentication recovery failed: {err}");
            return Ok(response);
        }
    }
}

fn normalize_create_body(route: &ResponsesRoute, body: Bytes) -> Bytes {
    if !route.accepts_response_input() {
        return body;
    }
    let Ok(mut payload) = serde_json::from_slice::<Value>(&body) else {
        return body;
    };
    let Some(payload_object) = payload.as_object_mut() else {
        return body;
    };

    let mut changed = false;
    if route.is_create() {
        payload_object.entry("store").or_insert_with(|| {
            changed = true;
            Value::Bool(false)
        });
    }
    if route.is_create() {
        let input = payload_object.entry("input").or_insert_with(|| {
            changed = true;
            Value::Array(Vec::new())
        });
        changed |= normalize_public_response_input(input);
    } else if let Some(input) = payload_object.get_mut("input") {
        changed |= normalize_public_response_input(input);
    }

    if !changed {
        return body;
    }
    serde_json::to_vec(&payload).map_or(body, Bytes::from)
}

fn normalize_public_response_input(input: &mut Value) -> bool {
    let mut changed = match input {
        Value::String(_) => {
            let text = input.take();
            *input = serde_json::json!([{
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": text}],
            }]);
            true
        }
        Value::Array(_) => false,
        _ => false,
    };
    if let Value::Array(items) = input {
        for item in items {
            changed |= normalize_public_response_input_item(item);
        }
    }
    changed
}

pub(crate) fn normalize_public_response_input_item(item: &mut Value) -> bool {
    let Some(item) = item.as_object_mut() else {
        return false;
    };
    if item.get("type").and_then(Value::as_str) == Some("message") || item.contains_key("role") {
        let mut changed = false;
        if !item.contains_key("type") {
            item.insert("type".to_string(), Value::String("message".to_string()));
            changed = true;
        }
        if let Some(text) = item
            .get("content")
            .and_then(Value::as_str)
            .map(str::to_string)
        {
            let content_type = if item.get("role").and_then(Value::as_str) == Some("assistant") {
                "output_text"
            } else {
                "input_text"
            };
            item.insert(
                "content".to_string(),
                serde_json::json!([{"type": content_type, "text": text}]),
            );
            changed = true;
        }
        if let Some(content) = item.get_mut("content").and_then(Value::as_array_mut) {
            for part in content {
                changed |= default_input_image_detail(part);
            }
        }
        return changed;
    }
    if matches!(
        item.get("type").and_then(Value::as_str),
        Some("function_call_output" | "custom_tool_call_output")
    ) && let Some(output) = item.get_mut("output").and_then(Value::as_array_mut)
    {
        let mut changed = false;
        for part in output {
            changed |= default_input_image_detail(part);
        }
        return changed;
    }
    false
}

fn default_input_image_detail(part: &mut Value) -> bool {
    let Some(part) = part.as_object_mut() else {
        return false;
    };
    if part.get("type").and_then(Value::as_str) == Some("input_image")
        && part.get("detail").is_none_or(Value::is_null)
    {
        part.insert("detail".to_string(), Value::String("auto".to_string()));
        return true;
    }
    false
}

fn validate_response_request_collections(
    route: &ResponsesRoute,
    body: &[u8],
) -> Result<(), crate::conversations::ConversationError> {
    if !route.accepts_response_input() {
        return Ok(());
    }
    let Ok(payload) = serde_json::from_slice::<Value>(body) else {
        return Ok(());
    };
    let Some(payload) = payload.as_object() else {
        return Ok(());
    };

    for field in ["context_management", "include"] {
        require_array_or_null(payload.get(field), field)?;
    }
    if route.is_create() {
        if payload.contains_key("tools") {
            require_array(payload.get("tools"), "tools")?;
        }
    } else {
        require_array_or_null(payload.get("tools"), "tools")?;
    }
    if let Some(instructions) = payload.get("instructions")
        && !instructions.is_null()
        && !instructions.is_string()
    {
        return Err(crate::conversations::ConversationError::invalid(
            "instructions",
            "`instructions` must be a string or null",
        ));
    }
    if let Some(input) = payload.get("input") {
        match input {
            Value::String(_) | Value::Array(_) => {}
            Value::Null if !route.is_create() => {}
            _ => {
                return Err(crate::conversations::ConversationError::invalid(
                    "input",
                    if route.is_create() {
                        "`input` must be a string or an array"
                    } else {
                        "`input` must be a string, an array, or null"
                    },
                ));
            }
        }
        if let Value::Array(items) = input {
            for (index, item) in items.iter().enumerate() {
                validate_response_input_item_collections(item, &format!("input[{index}]"))?;
            }
        }
    }
    if let Some(tool_choice) = payload.get("tool_choice").and_then(Value::as_object)
        && tool_choice.get("type").and_then(Value::as_str) == Some("allowed_tools")
    {
        require_array(tool_choice.get("tools"), "tool_choice.tools")?;
    }
    if let Some(tools) = payload.get("tools").and_then(Value::as_array) {
        for (index, tool) in tools.iter().enumerate() {
            validate_tool_collections(tool, &format!("tools[{index}]"))?;
        }
    }
    Ok(())
}

fn require_array_or_null(
    value: Option<&Value>,
    param: &str,
) -> Result<(), crate::conversations::ConversationError> {
    if value.is_some_and(|value| !value.is_array() && !value.is_null()) {
        return Err(crate::conversations::ConversationError::invalid(
            param,
            format!("`{param}` must be an array or null"),
        ));
    }
    Ok(())
}

fn require_nullable_array(
    value: Option<&Value>,
    param: &str,
) -> Result<(), crate::conversations::ConversationError> {
    if !value.is_some_and(|value| value.is_array() || value.is_null()) {
        return Err(crate::conversations::ConversationError::invalid(
            param,
            format!("`{param}` must be an array or null"),
        ));
    }
    Ok(())
}

fn require_array(
    value: Option<&Value>,
    param: &str,
) -> Result<(), crate::conversations::ConversationError> {
    if !value.is_some_and(Value::is_array) {
        return Err(crate::conversations::ConversationError::invalid(
            param,
            format!("`{param}` must be an array"),
        ));
    }
    Ok(())
}

fn validate_optional_array_field(
    object: &Map<String, Value>,
    field: &str,
    path: &str,
) -> Result<(), crate::conversations::ConversationError> {
    if object.contains_key(field) {
        require_array(object.get(field), &format!("{path}.{field}"))?;
    }
    Ok(())
}

fn validate_optional_nullable_array_field(
    object: &Map<String, Value>,
    field: &str,
    path: &str,
) -> Result<(), crate::conversations::ConversationError> {
    require_array_or_null(object.get(field), &format!("{path}.{field}"))
}

pub(crate) fn validate_response_input_item_collections(
    item: &Value,
    path: &str,
) -> Result<(), crate::conversations::ConversationError> {
    let item = item.as_object().ok_or_else(|| {
        crate::conversations::ConversationError::invalid(
            path,
            "Response input items must be objects",
        )
    })?;
    let item_type = item.get("type").and_then(Value::as_str);
    if item_type.is_none() && !item.contains_key("role") {
        return Err(crate::conversations::ConversationError::invalid(
            format!("{path}.type"),
            "Response input items require a string `type`",
        ));
    }
    if item_type == Some("message") || item.contains_key("role") {
        match item.get("content") {
            Some(Value::String(_) | Value::Array(_)) => {}
            _ => {
                return Err(crate::conversations::ConversationError::invalid(
                    format!("{path}.content"),
                    "Message `content` must be a string or an array",
                ));
            }
        }
        if let Some(parts) = item.get("content").and_then(Value::as_array) {
            for (index, part) in parts.iter().enumerate() {
                validate_content_part_collections(part, &format!("{path}.content[{index}]"))?;
            }
        }
    }
    match item_type {
        Some("reasoning") => {
            require_array(item.get("summary"), &format!("{path}.summary"))?;
            validate_optional_array_field(item, "content", path)?;
        }
        Some("file_search_call") => {
            require_array(item.get("queries"), &format!("{path}.queries"))?;
            validate_optional_nullable_array_field(item, "results", path)?;
        }
        Some("computer_call") => {
            require_array(
                item.get("pending_safety_checks"),
                &format!("{path}.pending_safety_checks"),
            )?;
            validate_optional_array_field(item, "actions", path)?;
            if let Some(action) = item.get("action").and_then(Value::as_object) {
                validate_computer_action_collections(action, &format!("{path}.action"))?;
            }
            if let Some(actions) = item.get("actions").and_then(Value::as_array) {
                for (index, action) in actions.iter().enumerate() {
                    if let Some(action) = action.as_object() {
                        validate_computer_action_collections(
                            action,
                            &format!("{path}.actions[{index}]"),
                        )?;
                    }
                }
            }
        }
        Some("computer_call_output") => {
            validate_optional_nullable_array_field(item, "acknowledged_safety_checks", path)?;
        }
        Some("web_search_call") => {
            if let Some(action) = item.get("action").and_then(Value::as_object) {
                validate_optional_array_field(action, "queries", &format!("{path}.action"))?;
                validate_optional_array_field(action, "sources", &format!("{path}.action"))?;
            }
        }
        Some("code_interpreter_call") => {
            require_nullable_array(item.get("outputs"), &format!("{path}.outputs"))?;
        }
        Some("local_shell_call") => {
            if let Some(action) = item.get("action").and_then(Value::as_object) {
                require_array(action.get("command"), &format!("{path}.action.command"))?;
            }
        }
        Some("shell_call") => {
            if let Some(action) = item.get("action").and_then(Value::as_object) {
                require_array(action.get("commands"), &format!("{path}.action.commands"))?;
            }
        }
        Some("shell_call_output") => {
            require_array(item.get("output"), &format!("{path}.output"))?;
        }
        Some("additional_tools" | "tool_search_output") => {
            require_array(item.get("tools"), &format!("{path}.tools"))?;
            if let Some(tools) = item.get("tools").and_then(Value::as_array) {
                for (index, tool) in tools.iter().enumerate() {
                    validate_tool_collections(tool, &format!("{path}.tools[{index}]"))?;
                }
            }
        }
        Some("mcp_list_tools") => {
            require_array(item.get("tools"), &format!("{path}.tools"))?;
        }
        Some("function_call_output" | "custom_tool_call_output") => {
            let output = item.get("output").ok_or_else(|| {
                crate::conversations::ConversationError::invalid(
                    format!("{path}.output"),
                    "Tool call `output` is required",
                )
            })?;
            if !output.is_string() && !output.is_array() {
                return Err(crate::conversations::ConversationError::invalid(
                    format!("{path}.output"),
                    "Tool call `output` must be a string or an array",
                ));
            }
            if let Some(parts) = item.get("output").and_then(Value::as_array) {
                for (index, part) in parts.iter().enumerate() {
                    validate_content_part_collections(part, &format!("{path}.output[{index}]"))?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_content_part_collections(
    part: &Value,
    path: &str,
) -> Result<(), crate::conversations::ConversationError> {
    let part = part.as_object().ok_or_else(|| {
        crate::conversations::ConversationError::invalid(path, "Content parts must be objects")
    })?;
    if !part.get("type").is_some_and(Value::is_string) {
        return Err(crate::conversations::ConversationError::invalid(
            format!("{path}.type"),
            "Content parts require a string `type`",
        ));
    }
    if part.get("type").and_then(Value::as_str) == Some("output_text") {
        require_array(part.get("annotations"), &format!("{path}.annotations"))?;
    } else {
        validate_optional_array_field(part, "annotations", path)?;
    }
    validate_optional_array_field(part, "logprobs", path)?;
    if let Some(logprobs) = part.get("logprobs").and_then(Value::as_array) {
        for (index, logprob) in logprobs.iter().enumerate() {
            let Some(logprob) = logprob.as_object() else {
                continue;
            };
            let logprob_path = format!("{path}.logprobs[{index}]");
            require_array(logprob.get("bytes"), &format!("{logprob_path}.bytes"))?;
            require_array(
                logprob.get("top_logprobs"),
                &format!("{logprob_path}.top_logprobs"),
            )?;
            if let Some(top_logprobs) = logprob.get("top_logprobs").and_then(Value::as_array) {
                for (top_index, top_logprob) in top_logprobs.iter().enumerate() {
                    let Some(top_logprob) = top_logprob.as_object() else {
                        continue;
                    };
                    require_array(
                        top_logprob.get("bytes"),
                        &format!("{logprob_path}.top_logprobs[{top_index}].bytes"),
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn validate_computer_action_collections(
    action: &Map<String, Value>,
    path: &str,
) -> Result<(), crate::conversations::ConversationError> {
    validate_optional_nullable_array_field(action, "keys", path)?;
    if action.get("type").and_then(Value::as_str) == Some("drag") {
        require_array(action.get("path"), &format!("{path}.path"))?;
    }
    Ok(())
}

fn validate_tool_collections(
    tool: &Value,
    path: &str,
) -> Result<(), crate::conversations::ConversationError> {
    let tool = tool.as_object().ok_or_else(|| {
        crate::conversations::ConversationError::invalid(path, "Tools must be objects")
    })?;
    if !tool.get("type").is_some_and(Value::is_string) {
        return Err(crate::conversations::ConversationError::invalid(
            format!("{path}.type"),
            "Tools require a string `type`",
        ));
    }
    validate_optional_nullable_array_field(tool, "allowed_callers", path)?;
    match tool.get("type").and_then(Value::as_str) {
        Some("file_search") => {
            require_array(
                tool.get("vector_store_ids"),
                &format!("{path}.vector_store_ids"),
            )?;
            if let Some(filters) = tool.get("filters") {
                validate_file_search_filter_collections(filters, &format!("{path}.filters"))?;
            }
        }
        Some("namespace") => {
            require_array(tool.get("tools"), &format!("{path}.tools"))?;
            if let Some(tools) = tool.get("tools").and_then(Value::as_array) {
                for (index, nested) in tools.iter().enumerate() {
                    validate_tool_collections(nested, &format!("{path}.tools[{index}]"))?;
                }
            }
        }
        Some("mcp") => {
            if let Some(allowed_tools) = tool.get("allowed_tools") {
                match allowed_tools {
                    Value::Array(_) | Value::Null => {}
                    Value::Object(filter) => {
                        validate_optional_array_field(
                            filter,
                            "tool_names",
                            &format!("{path}.allowed_tools"),
                        )?;
                    }
                    _ => {
                        return Err(crate::conversations::ConversationError::invalid(
                            format!("{path}.allowed_tools"),
                            "MCP `allowed_tools` must be an array, filter object, or null",
                        ));
                    }
                }
            }
            if let Some(require_approval) = tool.get("require_approval").and_then(Value::as_object)
            {
                for policy in ["always", "never"] {
                    if let Some(filter) = require_approval.get(policy).and_then(Value::as_object) {
                        validate_optional_array_field(
                            filter,
                            "tool_names",
                            &format!("{path}.require_approval.{policy}"),
                        )?;
                    }
                }
            }
        }
        Some("code_interpreter") => {
            if let Some(container) = tool.get("container").and_then(Value::as_object) {
                validate_container_collections(container, &format!("{path}.container"))?;
            }
        }
        Some("shell") => {
            if let Some(environment) = tool.get("environment").and_then(Value::as_object) {
                validate_container_collections(environment, &format!("{path}.environment"))?;
            }
        }
        Some(
            "web_search"
            | "web_search_2025_08_26"
            | "web_search_preview"
            | "web_search_preview_2025_03_11",
        ) => {
            validate_optional_array_field(tool, "search_content_types", path)?;
            if let Some(filters) = tool.get("filters").and_then(Value::as_object) {
                validate_optional_nullable_array_field(
                    filters,
                    "allowed_domains",
                    &format!("{path}.filters"),
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_container_collections(
    container: &Map<String, Value>,
    path: &str,
) -> Result<(), crate::conversations::ConversationError> {
    validate_optional_array_field(container, "file_ids", path)?;
    validate_optional_array_field(container, "skills", path)?;
    if let Some(network_policy) = container.get("network_policy").and_then(Value::as_object) {
        if network_policy.get("type").and_then(Value::as_str) == Some("allowlist") {
            require_array(
                network_policy.get("allowed_domains"),
                &format!("{path}.network_policy.allowed_domains"),
            )?;
        } else {
            validate_optional_array_field(
                network_policy,
                "allowed_domains",
                &format!("{path}.network_policy"),
            )?;
        }
        validate_optional_array_field(
            network_policy,
            "domain_secrets",
            &format!("{path}.network_policy"),
        )?;
    }
    Ok(())
}

fn validate_file_search_filter_collections(
    filter: &Value,
    path: &str,
) -> Result<(), crate::conversations::ConversationError> {
    let Some(filter) = filter.as_object() else {
        return Ok(());
    };
    if matches!(
        filter.get("type").and_then(Value::as_str),
        Some("and" | "or")
    ) {
        require_array(filter.get("filters"), &format!("{path}.filters"))?;
        if let Some(filters) = filter.get("filters").and_then(Value::as_array) {
            for (index, nested) in filters.iter().enumerate() {
                validate_file_search_filter_collections(
                    nested,
                    &format!("{path}.filters[{index}]"),
                )?;
            }
        }
    }
    Ok(())
}

pub(crate) fn normalize_response_item_collections(item: &mut Value) -> bool {
    let Some(message) = item.as_object_mut() else {
        return false;
    };
    let item_type = message
        .get("type")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let mut changed = false;

    if item_type.as_deref() == Some("message") || message.contains_key("role") {
        message
            .entry("type")
            .or_insert_with(|| Value::String("message".to_string()));
        changed |= normalize_message_content(message);
    }

    match item_type.as_deref() {
        Some("reasoning") => {
            changed |= normalize_typed_text_collection(message, "summary", "summary_text");
            changed |= normalize_typed_text_collection(message, "content", "reasoning_text");
        }
        Some("file_search_call") => {
            changed |= normalize_array_field(message, "queries");
            changed |= normalize_nullable_array_field(message, "results");
        }
        Some("computer_call") => {
            changed |= normalize_array_field(message, "pending_safety_checks");
            changed |= normalize_computer_actions(message);
        }
        Some("computer_call_output") => {
            changed |= normalize_nullable_array_field(message, "acknowledged_safety_checks");
        }
        Some("web_search_call") => {
            if let Some(action) = message.get_mut("action").and_then(Value::as_object_mut) {
                changed |= normalize_array_field(action, "queries");
                changed |= normalize_array_field(action, "sources");
            }
        }
        Some("code_interpreter_call") => {
            changed |= normalize_nullable_array_field(message, "outputs");
        }
        Some("local_shell_call") => {
            if let Some(action) = message.get_mut("action").and_then(Value::as_object_mut) {
                changed |= normalize_array_field(action, "command");
            }
        }
        Some("shell_call") => {
            if let Some(action) = message.get_mut("action").and_then(Value::as_object_mut) {
                changed |= normalize_array_field(action, "commands");
            }
        }
        Some("shell_call_output") => {
            changed |= normalize_array_field(message, "output");
        }
        Some("additional_tools" | "tool_search_output") => {
            changed |= normalize_tools_field(message, "tools");
        }
        Some("mcp_list_tools") => {
            // MCP tool annotations are arbitrary objects, unlike output_text
            // annotations. Only the outer tool collection is normalized here.
            changed |= normalize_array_field(message, "tools");
        }
        Some("function_call_output" | "custom_tool_call_output") => {
            changed |= normalize_string_or_content_array_field(message, "output");
        }
        _ => {}
    }

    changed
}

fn normalize_message_content(message: &mut Map<String, Value>) -> bool {
    let role_is_assistant = message.get("role").and_then(Value::as_str) == Some("assistant");
    let Some(content) = message.get_mut("content") else {
        return false;
    };
    let original = content.clone();
    let candidates = match content.take() {
        Value::Array(values) => values,
        Value::Object(values) => {
            let direct =
                normalize_message_content_part(Value::Object(values.clone()), role_is_assistant);
            match direct {
                Some(value) => vec![value],
                None => values.into_values().collect(),
            }
        }
        Value::Null => Vec::new(),
        value => vec![value],
    };
    let mut parts = candidates
        .into_iter()
        .filter_map(|part| normalize_message_content_part(part, role_is_assistant))
        .collect::<Vec<_>>();
    let mut changed = Value::Array(parts.clone()) != original;
    for part in &mut parts {
        changed |= normalize_content_part_collections(part);
    }
    *content = Value::Array(parts);
    changed
}

fn normalize_message_content_part(mut part: Value, role_is_assistant: bool) -> Option<Value> {
    if let Value::String(text) = part {
        let content_type = if role_is_assistant {
            "output_text"
        } else {
            "input_text"
        };
        return Some(serde_json::json!({"type": content_type, "text": text}));
    }
    let part_object = part.as_object_mut()?;
    match part_object.get("type") {
        Some(Value::String(part_type)) => {
            if part_type == "input_image" && part_object.get("detail").is_none_or(Value::is_null) {
                part_object.insert("detail".to_string(), Value::String("auto".to_string()));
            }
            return Some(part);
        }
        Some(value) if !value.is_null() => return None,
        _ => {}
    }

    let inferred_type = if part_object.get("text").and_then(Value::as_str).is_some() {
        if role_is_assistant {
            "output_text"
        } else {
            "input_text"
        }
    } else if part_object.contains_key("image_url") {
        part_object
            .entry("detail")
            .or_insert_with(|| Value::String("auto".to_string()));
        "input_image"
    } else if ["file_data", "file_id", "file_url", "filename"]
        .iter()
        .any(|field| part_object.contains_key(*field))
    {
        "input_file"
    } else {
        return None;
    };
    part_object.insert("type".to_string(), Value::String(inferred_type.to_string()));
    Some(part)
}

fn normalize_content_part_collections(part: &mut Value) -> bool {
    let Some(part) = part.as_object_mut() else {
        return false;
    };
    let mut changed = normalize_annotation_collection(part);
    changed |= normalize_array_field(part, "logprobs");
    if let Some(logprobs) = part.get_mut("logprobs").and_then(Value::as_array_mut) {
        for logprob in logprobs {
            changed |= normalize_logprob_collections(logprob);
        }
    }
    changed
}

fn normalize_annotation_collection(content: &mut Map<String, Value>) -> bool {
    let Some(annotations) = content.get_mut("annotations") else {
        return false;
    };

    match annotations {
        Value::Array(values) => {
            let original_len = values.len();
            values.retain(is_typed_annotation);
            values.len() != original_len
        }
        Value::Object(annotation) if has_string_type(annotation) => {
            let annotation = annotations.take();
            *annotations = Value::Array(vec![annotation]);
            true
        }
        Value::Object(annotation_map) => {
            let values = std::mem::take(annotation_map)
                .into_values()
                .filter(is_typed_annotation)
                .collect();
            *annotations = Value::Array(values);
            true
        }
        _ => {
            *annotations = Value::Array(Vec::new());
            true
        }
    }
}

fn is_typed_annotation(annotation: &Value) -> bool {
    annotation.as_object().is_some_and(has_string_type)
}

fn has_string_type(annotation: &Map<String, Value>) -> bool {
    annotation.get("type").and_then(Value::as_str).is_some()
}

fn normalize_typed_text_collection(
    object: &mut Map<String, Value>,
    field: &str,
    expected_type: &'static str,
) -> bool {
    let Some(value) = object.get_mut(field) else {
        return false;
    };
    let original = value.clone();
    let values = match value.take() {
        Value::Array(values) => values
            .into_iter()
            .filter_map(|value| normalize_typed_text_part(value, expected_type))
            .collect(),
        Value::Object(values) => {
            let direct = normalize_typed_text_part(Value::Object(values.clone()), expected_type);
            match direct {
                Some(value) => vec![value],
                None => values
                    .into_values()
                    .filter_map(|value| normalize_typed_text_part(value, expected_type))
                    .collect(),
            }
        }
        Value::String(text) => vec![serde_json::json!({
            "type": expected_type,
            "text": text,
        })],
        _ => Vec::new(),
    };
    *value = Value::Array(values);
    *value != original
}

fn normalize_typed_text_part(value: Value, expected_type: &'static str) -> Option<Value> {
    match value {
        Value::String(text) => Some(serde_json::json!({
            "type": expected_type,
            "text": text,
        })),
        Value::Object(mut part)
            if part.get("text").and_then(Value::as_str).is_some()
                && matches!(
                    part.get("type"),
                    None | Some(Value::Null) | Some(Value::String(_))
                ) =>
        {
            match part.get("type").and_then(Value::as_str) {
                Some(actual_type) if actual_type != expected_type => return None,
                Some(_) => {}
                None => {
                    part.insert("type".to_string(), Value::String(expected_type.to_string()));
                }
            }
            Some(Value::Object(part))
        }
        _ => None,
    }
}

fn normalize_logprob_collections(logprob: &mut Value) -> bool {
    let Some(logprob) = logprob.as_object_mut() else {
        return false;
    };
    let mut changed = normalize_array_field(logprob, "bytes");
    changed |= normalize_array_field(logprob, "top_logprobs");
    if let Some(top_logprobs) = logprob
        .get_mut("top_logprobs")
        .and_then(Value::as_array_mut)
    {
        for top_logprob in top_logprobs {
            if let Some(top_logprob) = top_logprob.as_object_mut() {
                changed |= normalize_array_field(top_logprob, "bytes");
            }
        }
    }
    changed
}

fn normalize_computer_actions(item: &mut Map<String, Value>) -> bool {
    let mut changed = false;
    if let Some(action) = item.get_mut("action").and_then(Value::as_object_mut) {
        changed |= normalize_computer_action(action);
    }
    changed |= normalize_array_field(item, "actions");
    if let Some(actions) = item.get_mut("actions").and_then(Value::as_array_mut) {
        for action in actions {
            if let Some(action) = action.as_object_mut() {
                changed |= normalize_computer_action(action);
            }
        }
    }
    changed
}

fn normalize_computer_action(action: &mut Map<String, Value>) -> bool {
    let mut changed = normalize_nullable_array_field(action, "keys");
    if action.get("type").and_then(Value::as_str) == Some("drag") {
        changed |= normalize_array_field(action, "path");
    }
    changed
}

fn normalize_tools_field(object: &mut Map<String, Value>, field: &str) -> bool {
    let mut changed = normalize_array_field(object, field);
    if let Some(tools) = object.get_mut(field).and_then(Value::as_array_mut) {
        for tool in tools {
            changed |= normalize_tool_collections(tool);
        }
    }
    changed
}

fn normalize_tool_collections(tool: &mut Value) -> bool {
    let Some(tool) = tool.as_object_mut() else {
        return false;
    };
    let tool_type = tool.get("type").and_then(Value::as_str).map(str::to_owned);
    let mut changed = normalize_nullable_array_field(tool, "allowed_callers");

    match tool_type.as_deref() {
        Some("file_search") => {
            changed |= normalize_array_field(tool, "vector_store_ids");
            if let Some(filters) = tool.get_mut("filters") {
                changed |= normalize_file_search_filter(filters);
            }
        }
        Some("namespace") => {
            changed |= normalize_tools_field(tool, "tools");
        }
        Some("mcp") => {
            if let Some(allowed_tools) = tool.get_mut("allowed_tools") {
                match allowed_tools {
                    Value::Object(filter) => {
                        changed |= normalize_array_field(filter, "tool_names");
                    }
                    Value::Array(_) => {}
                    Value::Null => {}
                    _ => {
                        let allowed_tool = allowed_tools.take();
                        *allowed_tools = Value::Array(vec![allowed_tool]);
                        changed = true;
                    }
                }
            }
            if let Some(require_approval) = tool
                .get_mut("require_approval")
                .and_then(Value::as_object_mut)
            {
                for policy in ["always", "never"] {
                    if let Some(filter) = require_approval
                        .get_mut(policy)
                        .and_then(Value::as_object_mut)
                    {
                        changed |= normalize_array_field(filter, "tool_names");
                    }
                }
            }
        }
        Some("code_interpreter") => {
            if let Some(container) = tool.get_mut("container").and_then(Value::as_object_mut) {
                changed |= normalize_container_collections(container);
            }
        }
        Some("shell") => {
            if let Some(environment) = tool.get_mut("environment").and_then(Value::as_object_mut) {
                changed |= normalize_container_collections(environment);
            }
        }
        Some(
            "web_search"
            | "web_search_2025_08_26"
            | "web_search_preview"
            | "web_search_preview_2025_03_11",
        ) => {
            changed |= normalize_array_field(tool, "search_content_types");
            if let Some(filters) = tool.get_mut("filters").and_then(Value::as_object_mut) {
                changed |= normalize_nullable_array_field(filters, "allowed_domains");
            }
        }
        _ => {}
    }
    changed
}

fn normalize_file_search_filter(filter: &mut Value) -> bool {
    let Some(filter) = filter.as_object_mut() else {
        return false;
    };
    if !matches!(
        filter.get("type").and_then(Value::as_str),
        Some("and" | "or")
    ) {
        return false;
    }
    let mut changed = normalize_array_field(filter, "filters");
    if let Some(filters) = filter.get_mut("filters").and_then(Value::as_array_mut) {
        for filter in filters {
            changed |= normalize_file_search_filter(filter);
        }
    }
    changed
}

fn normalize_container_collections(container: &mut Map<String, Value>) -> bool {
    let mut changed = normalize_array_field(container, "file_ids");
    changed |= normalize_array_field(container, "skills");
    if let Some(network_policy) = container
        .get_mut("network_policy")
        .and_then(Value::as_object_mut)
    {
        changed |= normalize_array_field(network_policy, "allowed_domains");
        changed |= normalize_array_field(network_policy, "domain_secrets");
    }
    changed
}

fn normalize_array_field(object: &mut Map<String, Value>, field: &str) -> bool {
    let Some(value) = object.get_mut(field) else {
        return false;
    };
    match value {
        Value::Array(_) => false,
        Value::Null => {
            *value = Value::Array(Vec::new());
            true
        }
        Value::Object(object) if object.is_empty() => {
            *value = Value::Array(Vec::new());
            true
        }
        _ => {
            let item = value.take();
            *value = Value::Array(vec![item]);
            true
        }
    }
}

fn normalize_nullable_array_field(object: &mut Map<String, Value>, field: &str) -> bool {
    if object.get(field).is_some_and(Value::is_null) {
        return false;
    }
    normalize_array_field(object, field)
}

fn normalize_string_or_content_array_field(object: &mut Map<String, Value>, field: &str) -> bool {
    let Some(value) = object.get_mut(field) else {
        return false;
    };
    if value.is_string() {
        return false;
    }
    let original = value.clone();
    let candidates = match value.take() {
        Value::Array(values) => values,
        Value::Null => Vec::new(),
        value => vec![value],
    };
    *value = Value::Array(
        candidates
            .into_iter()
            .filter_map(|part| normalize_message_content_part(part, false))
            .collect(),
    );
    *value != original
}

fn require_chatgpt_auth(auth: Option<CodexAuth>) -> Result<CodexAuth> {
    match auth {
        Some(auth @ (CodexAuth::Chatgpt(_) | CodexAuth::ChatgptAuthTokens(_))) => Ok(auth),
        Some(_) => Err(anyhow!(
            "--auth chatgpt requires a ChatGPT login; run `codex login` first"
        )),
        None => Err(anyhow!(
            "no Codex authentication found; run `codex login` first"
        )),
    }
}

fn extend_forwarded_headers(destination: &mut HeaderMap, incoming: &HeaderMap) {
    for (name, value) in incoming {
        if !is_filtered_request_header(name) {
            destination.append(name, value.clone());
        }
    }
}

fn is_filtered_request_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "authorization"
            | "connection"
            | "content-length"
            | "host"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

pub(crate) fn upstream_response(
    response: reqwest::Response,
    exchange_dump: Option<crate::dump::ExchangeDump>,
) -> Response {
    let status = response.status();
    let upstream_headers = response.headers().clone();
    let response_stream = response.bytes_stream();
    let body = match exchange_dump {
        Some(exchange_dump) => Body::from_stream(exchange_dump.tee_response_stream(
            status.as_u16(),
            &upstream_headers,
            response_stream,
        )),
        None => Body::from_stream(response_stream),
    };
    let mut downstream = Response::new(body);
    *downstream.status_mut() = status;
    for (name, value) in &upstream_headers {
        if !is_filtered_response_header(name) {
            downstream.headers_mut().append(name, value.clone());
        }
    }
    downstream
}

pub(crate) fn is_filtered_response_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection" | "content-length" | "trailer" | "transfer-encoding" | "upgrade"
    )
}

async fn shutdown_server(State(state): State<ChatgptState>) -> StatusCode {
    state.shutdown.cancel();
    StatusCode::OK
}

#[derive(Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    message: String,
    r#type: &'static str,
    code: &'static str,
}

fn error_response(status: StatusCode, message: String) -> Response {
    (
        status,
        axum::Json(ErrorEnvelope {
            error: ErrorBody {
                message,
                r#type: "proxy_error",
                code: "upstream_request_failed",
            },
        }),
    )
        .into_response()
}

#[cfg(test)]
#[path = "chatgpt_tests.rs"]
mod tests;
