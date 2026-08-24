use axum::body::to_bytes;
use codex_http_client::HttpClient;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use http::HeaderMap;
use http::Method;
use http::StatusCode;
use pretty_assertions::assert_eq;
use tokio_util::sync::CancellationToken;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::header;
use wiremock::matchers::method;
use wiremock::matchers::path;
use wiremock::matchers::query_param;

use super::handle;
use super::models_upstream_url;
use crate::chatgpt::ChatgptState;

fn state(server: &MockServer) -> ChatgptState {
    ChatgptState {
        auth_manager: AuthManager::from_auth_for_testing(
            CodexAuth::create_dummy_chatgpt_auth_for_testing(),
        ),
        client: HttpClient::new(reqwest::Client::new()),
        upstream_url: format!("{}/backend-api/codex/responses", server.uri())
            .parse()
            .expect("upstream URL"),
        upstream_headers: HeaderMap::new(),
        dump_dir: None,
        chat_completions_compat: true,
        conversations: None,
        shutdown: CancellationToken::new(),
    }
}

fn backend_catalog() -> serde_json::Value {
    serde_json::json!({
        "models": [
            {"slug":"gpt-5.6-sol","visibility":"list","supported_in_api":true},
            {"slug":"gpt-5.6-terra","visibility":"list","supported_in_api":true},
            {"slug":"gpt-5.6-luna","visibility":"list","supported_in_api":true},
            {"slug":"gpt-daybreak-blue-latest","visibility":"list","supported_in_api":true},
            {"slug":"gpt-5.5","visibility":"list","supported_in_api":true},
            {"slug":"gpt-5.4","visibility":"list","supported_in_api":true},
            {"slug":"gpt-5.4-mini","visibility":"list","supported_in_api":true},
            {"slug":"gpt-5.3-codex-spark","visibility":"list","supported_in_api":false},
            {"slug":"codex-auto-review","visibility":"hide","supported_in_api":true},
        ]
    })
}

async fn mount_catalog(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/backend-api/codex/models"))
        .and(query_param("client_version", "0.0.0"))
        .and(header("authorization", "Bearer Access Token"))
        .and(header("chatgpt-account-id", "account_id"))
        .respond_with(ResponseTemplate::new(200).set_body_json(backend_catalog()))
        .mount(server)
        .await;
}

async fn json_body(response: axum::response::Response) -> serde_json::Value {
    serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body"),
    )
    .expect("JSON response")
}

#[tokio::test]
async fn lists_models_from_the_dynamic_catalog_without_requiring_instructions() {
    let server = MockServer::start().await;
    mount_catalog(&server).await;

    let response = handle(
        &state(&server),
        &Method::GET,
        "/v1/models",
        &HeaderMap::new(),
    )
    .await
    .expect("Models route");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        json_body(response).await,
        serde_json::json!({
            "object": "list",
            "data": [
                {"id":"gpt-5.6-sol","object":"model","created":0,"owned_by":"openai"},
                {"id":"gpt-5.6-terra","object":"model","created":0,"owned_by":"openai"},
                {"id":"gpt-5.6-luna","object":"model","created":0,"owned_by":"openai"},
                {"id":"gpt-daybreak-blue-latest","object":"model","created":0,"owned_by":"openai"},
                {"id":"gpt-5.5","object":"model","created":0,"owned_by":"openai"},
                {"id":"gpt-5.4","object":"model","created":0,"owned_by":"openai"},
                {"id":"gpt-5.4-mini","object":"model","created":0,"owned_by":"openai"},
            ],
        })
    );
}

#[tokio::test]
async fn retrieves_only_api_visible_models_from_the_dynamic_catalog() {
    let server = MockServer::start().await;
    mount_catalog(&server).await;
    let state = state(&server);

    let response = handle(
        &state,
        &Method::GET,
        "/v1/models/gpt-daybreak-blue-latest",
        &HeaderMap::new(),
    )
    .await
    .expect("model retrieve route");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json_body(response).await["id"], "gpt-daybreak-blue-latest");

    let response = handle(
        &state,
        &Method::GET,
        "/v1/models/gpt-5.3-codex-spark",
        &HeaderMap::new(),
    )
    .await
    .expect("model retrieve route");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn rejects_unlisted_models_with_the_public_error_shape() {
    let server = MockServer::start().await;
    mount_catalog(&server).await;

    let response = handle(
        &state(&server),
        &Method::GET,
        "/v1/models/gpt-5.2",
        &HeaderMap::new(),
    )
    .await
    .expect("model retrieve route");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        json_body(response).await,
        serde_json::json!({
            "error": {
                "message": "The model `gpt-5.2` does not exist or you do not have access to it.",
                "type": "invalid_request_error",
                "param": "model",
                "code": "model_not_found",
            }
        })
    );
}

#[tokio::test]
async fn recognizes_only_models_paths_and_get_operations() {
    let server = MockServer::start().await;
    let state = state(&server);

    assert!(
        handle(&state, &Method::GET, "/v1/models-other", &HeaderMap::new())
            .await
            .is_none()
    );
    assert_eq!(
        handle(
            &state,
            &Method::DELETE,
            "/v1/models/gpt-5.5",
            &HeaderMap::new(),
        )
        .await
        .expect("Models route")
        .status(),
        StatusCode::METHOD_NOT_ALLOWED
    );
}

#[test]
fn derives_the_models_route_and_preserves_provider_query_parameters() {
    let responses_url =
        "https://chatgpt.com/backend-api/codex/responses?tenant=codex&client_version=old"
            .parse()
            .expect("Responses URL");

    assert_eq!(
        models_upstream_url(&responses_url)
            .expect("Models URL")
            .as_str(),
        "https://chatgpt.com/backend-api/codex/models?tenant=codex&client_version=0.0.0"
    );
}
