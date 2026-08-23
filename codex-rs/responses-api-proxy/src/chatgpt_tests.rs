use axum::body::Bytes;
use codex_http_client::HttpClient;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use http::HeaderMap;
use http::HeaderValue;
use pretty_assertions::assert_eq;
use tokio_util::sync::CancellationToken;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path;

use super::ChatgptState;
use super::forward_request;

#[tokio::test]
async fn forwards_with_managed_chatgpt_auth_and_replaces_incoming_authorization() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            r#"{"id":"resp_test","object":"response"}"#,
            "application/json",
        ))
        .mount(&server)
        .await;
    let auth_manager =
        AuthManager::from_auth_for_testing(CodexAuth::create_dummy_chatgpt_auth_for_testing());
    let mut upstream_headers = HeaderMap::new();
    upstream_headers.insert("version", HeaderValue::from_static("test-version"));
    let state = ChatgptState {
        auth_manager,
        client: HttpClient::new(reqwest::Client::new()),
        upstream_url: format!("{}/backend-api/codex/responses", server.uri()),
        upstream_headers,
        dump_dir: None,
        shutdown: CancellationToken::new(),
    };
    let mut incoming_headers = HeaderMap::new();
    incoming_headers.insert(
        http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer caller-token"),
    );
    incoming_headers.insert("x-client-request-id", HeaderValue::from_static("thread-1"));
    let body = Bytes::from_static(br#"{"model":"gpt-test","input":"hello"}"#);

    let response = forward_request(&state, incoming_headers, body.clone())
        .await
        .expect("request should be forwarded");
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        response.bytes().await.expect("response body"),
        Bytes::from_static(br#"{"id":"resp_test","object":"response"}"#)
    );
    let requests = server
        .received_requests()
        .await
        .expect("request recording should be enabled");
    let request = requests.first().expect("one upstream request");
    assert_eq!(requests.len(), 1);
    assert_eq!(request.body, body);
    assert_eq!(
        request
            .headers
            .get(http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
        Some("Bearer Access Token")
    );
    assert_eq!(
        request
            .headers
            .get("chatgpt-account-id")
            .and_then(|value| value.to_str().ok()),
        Some("account_id")
    );
    assert_eq!(
        request
            .headers
            .get("version")
            .and_then(|value| value.to_str().ok()),
        Some("test-version")
    );
    assert_eq!(
        request
            .headers
            .get("x-client-request-id")
            .and_then(|value| value.to_str().ok()),
        Some("thread-1")
    );
}
