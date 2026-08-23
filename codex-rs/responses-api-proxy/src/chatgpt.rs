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
use axum::routing::post;
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
use crate::dump::ExchangeDumper;
use crate::write_server_info;

#[derive(Clone)]
struct ChatgptState {
    auth_manager: Arc<AuthManager>,
    client: HttpClient,
    upstream_url: String,
    upstream_headers: HeaderMap,
    dump_dir: Option<Arc<ExchangeDumper>>,
    shutdown: CancellationToken,
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
        .context("parsing --upstream-url or the configured provider URL")?
        .to_string();
    let client = create_client_for_route_async(
        config.http_client_factory(),
        upstream_url.clone(),
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
        shutdown: shutdown.clone(),
    };
    let router = Router::new()
        .route("/v1/responses", post(responses))
        .fallback(forbidden);
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
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if uri.query().is_some() {
        return StatusCode::FORBIDDEN.into_response();
    }

    let exchange_dump = state.dump_dir.as_deref().and_then(|dump_dir| {
        dump_dir
            .dump_http_request(&Method::POST, uri.path(), &headers, &body)
            .map_err(|err| {
                eprintln!("responses-api-proxy failed to dump request: {err}");
                err
            })
            .ok()
    });
    match forward_request(&state, headers, body).await {
        Ok(response) => upstream_response(response, exchange_dump),
        Err(err) => {
            eprintln!("forwarding error: {err:#}");
            error_response(StatusCode::BAD_GATEWAY, err.to_string())
        }
    }
}

async fn forward_request(
    state: &ChatgptState,
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
            .post(&state.upstream_url)
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

fn upstream_response(
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

fn is_filtered_response_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection" | "content-length" | "trailer" | "transfer-encoding" | "upgrade"
    )
}

async fn forbidden() -> StatusCode {
    StatusCode::FORBIDDEN
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
