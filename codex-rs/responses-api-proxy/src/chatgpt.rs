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
use tokio_util::sync::CancellationToken;

use crate::Args;
use crate::conversations::ConversationStore;
use crate::dump::ExchangeDumper;
use crate::routes::ResponsesRoute;
use crate::routes::resolve_responses_route;
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
    let listener =
        tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, args.port.unwrap_or(0)))
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
            .request(route.method.clone(), upstream_url.clone())
            .headers(headers)
            .body(body.clone())
            .send()
            .await
            .context("forwarding request to the Codex backend")?;
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
    if !route.is_create() {
        return body;
    }
    let Ok(mut payload) = serde_json::from_slice::<serde_json::Value>(&body) else {
        return body;
    };
    let Some(payload_object) = payload.as_object_mut() else {
        return body;
    };

    let mut changed = false;
    payload_object.entry("store").or_insert_with(|| {
        changed = true;
        serde_json::Value::Bool(false)
    });
    let input = payload_object.entry("input").or_insert_with(|| {
        changed = true;
        serde_json::Value::Array(Vec::new())
    });
    changed |= match input {
        serde_json::Value::String(_) => {
            let text = input.take();
            *input = serde_json::json!([{
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": text}],
            }]);
            true
        }
        serde_json::Value::Array(items) => {
            let mut changed = false;
            for item in items {
                changed |= normalize_easy_message(item);
            }
            changed
        }
        serde_json::Value::Null => {
            *input = serde_json::Value::Array(Vec::new());
            true
        }
        _ => false,
    };

    if !changed {
        return body;
    }
    serde_json::to_vec(&payload).map_or(body, Bytes::from)
}

fn normalize_easy_message(item: &mut serde_json::Value) -> bool {
    let Some(message) = item.as_object_mut() else {
        return false;
    };
    if !message.contains_key("role") {
        return false;
    }
    let Some(serde_json::Value::String(_)) = message.get("content") else {
        return false;
    };

    let text = message
        .get_mut("content")
        .map(serde_json::Value::take)
        .unwrap_or(serde_json::Value::Null);
    message
        .entry("type")
        .or_insert_with(|| serde_json::Value::String("message".to_string()));
    message.insert(
        "content".to_string(),
        serde_json::json!([{"type": "input_text", "text": text}]),
    );
    true
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
