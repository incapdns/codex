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
use crate::routes::resolve_responses_route;

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
        upstream_url: format!("{}/backend-api/codex/responses", server.uri())
            .parse()
            .unwrap(),
        upstream_headers,
        dump_dir: None,
        chat_completions_compat: true,
        shutdown: CancellationToken::new(),
    };
    let mut incoming_headers = HeaderMap::new();
    incoming_headers.insert(
        http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer caller-token"),
    );
    incoming_headers.insert("x-client-request-id", HeaderValue::from_static("thread-1"));
    let body = Bytes::from_static(br#"{"model":"gpt-test","input":"hello"}"#);

    let route = resolve_responses_route("POST", "/v1/responses").unwrap();
    let response = forward_request(&state, &route, incoming_headers, body.clone())
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
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&request.body).unwrap(),
        serde_json::json!({
            "model": "gpt-test",
            "store": false,
            "input": [{
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "hello"}],
            }],
        })
    );
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

#[test]
fn normalizes_easy_message_content_but_preserves_structured_input() {
    let route = resolve_responses_route("POST", "/v1/responses").unwrap();
    let easy =
        Bytes::from_static(br#"{"input":[{"role":"user","content":"hello"}],"model":"gpt-test"}"#);
    let normalized = super::normalize_create_body(&route, easy);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&normalized).unwrap(),
        serde_json::json!({
            "model": "gpt-test",
            "store": false,
            "input": [{
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "hello"}],
            }],
        })
    );

    let structured = Bytes::from_static(
        br#"{"model":"gpt-test","store":false,"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}]}"#,
    );
    assert_eq!(
        super::normalize_create_body(&route, structured.clone()),
        structured
    );

    let explicit_store = Bytes::from_static(br#"{"model":"gpt-test","store":true,"input":[]}"#);
    assert_eq!(
        super::normalize_create_body(&route, explicit_store.clone()),
        explicit_store
    );

    let omitted_input = Bytes::from_static(br#"{"model":"gpt-test"}"#);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&super::normalize_create_body(
            &route,
            omitted_input,
        ))
        .unwrap(),
        serde_json::json!({"model": "gpt-test", "store": false, "input": []})
    );
}

#[tokio::test]
async fn forwards_every_supported_responses_operation() {
    let server = MockServer::start().await;
    let cases = [
        ("POST", "/v1/responses", "/backend-api/codex/responses"),
        (
            "GET",
            "/v1/responses/resp_123?starting_after=7",
            "/backend-api/codex/responses/resp_123?starting_after=7",
        ),
        (
            "DELETE",
            "/v1/responses/resp_123",
            "/backend-api/codex/responses/resp_123",
        ),
        (
            "POST",
            "/v1/responses/resp_123/cancel",
            "/backend-api/codex/responses/resp_123/cancel",
        ),
        (
            "POST",
            "/v1/responses/compact",
            "/backend-api/codex/responses/compact",
        ),
        (
            "POST",
            "/v1/responses/input_tokens",
            "/backend-api/codex/responses/input_tokens",
        ),
        (
            "GET",
            "/v1/responses/resp_123/input_items?limit=20&order=desc",
            "/backend-api/codex/responses/resp_123/input_items?limit=20&order=desc",
        ),
    ];
    for (method_name, _, expected_uri) in cases {
        let expected_url = reqwest::Url::parse(&format!("{}{}", server.uri(), expected_uri))
            .expect("valid mock URL");
        Mock::given(method(method_name))
            .and(path(expected_url.path()))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
    }

    let state = ChatgptState {
        auth_manager: AuthManager::from_auth_for_testing(
            CodexAuth::create_dummy_chatgpt_auth_for_testing(),
        ),
        client: HttpClient::new(reqwest::Client::new()),
        upstream_url: format!("{}/backend-api/codex/responses", server.uri())
            .parse()
            .unwrap(),
        upstream_headers: HeaderMap::new(),
        dump_dir: None,
        chat_completions_compat: true,
        shutdown: CancellationToken::new(),
    };

    for (method_name, local_uri, _) in cases {
        let route = resolve_responses_route(method_name, local_uri).expect(local_uri);
        let response = forward_request(&state, &route, HeaderMap::new(), Bytes::new())
            .await
            .expect("request should be forwarded");
        assert_eq!(response.status(), http::StatusCode::OK);
    }

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), cases.len());
    for (request, (method_name, _, expected_uri)) in requests.iter().zip(cases) {
        assert_eq!(request.method.as_str(), method_name);
        let actual_uri = match request.url.query() {
            Some(query) => format!("{}?{query}", request.url.path()),
            None => request.url.path().to_string(),
        };
        assert_eq!(actual_uri, expected_uri);
    }
}
