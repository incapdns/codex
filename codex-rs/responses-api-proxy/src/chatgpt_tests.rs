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
        conversations: None,
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

#[test]
fn normalizes_singletons_for_schema_declared_response_collections() {
    let route = resolve_responses_route("POST", "/v1/responses").unwrap();
    let body = Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "model": "gpt-test",
            "context_management": {"type": "compaction"},
            "include": "reasoning.encrypted_content",
            "tool_choice": {
                "type": "allowed_tools",
                "mode": "auto",
                "tools": {"type": "function", "name": "selected"}
            },
            "tools": {
                "type": "namespace",
                "name": "demo",
                "description": "demo",
                "allowed_callers": "direct",
                "tools": {
                    "type": "file_search",
                    "vector_store_ids": "vs_1"
                }
            },
            "input": [
                {
                    "type": "message",
                    "role": "assistant",
                    "content": {
                        "type": "output_text",
                        "text": "answer",
                        "annotations": {"type": "file_citation", "file_id": "file_1"},
                        "logprobs": {
                            "token": "a",
                            "bytes": 97,
                            "top_logprobs": {"token": "b", "bytes": 98}
                        }
                    }
                },
                {
                    "type": "reasoning",
                    "id": "rs_1",
                    "summary": {"type": "summary_text", "text": "summary"},
                    "content": {"type": "reasoning_text", "text": "detail"}
                },
                {
                    "type": "file_search_call",
                    "id": "fs_1",
                    "queries": "needle",
                    "results": {"file_id": "file_1"}
                },
                {
                    "type": "computer_call",
                    "id": "pc_1",
                    "pending_safety_checks": {"id": "safe_1"},
                    "action": {"type": "drag", "keys": "SHIFT", "path": {"x": 1, "y": 2}},
                    "actions": {"type": "keypress", "keys": "ENTER"}
                },
                {
                    "type": "computer_call_output",
                    "call_id": "pc_1",
                    "acknowledged_safety_checks": {"id": "safe_1"}
                },
                {
                    "type": "web_search_call",
                    "id": "web_1",
                    "action": {
                        "type": "search",
                        "queries": "query",
                        "sources": {"type": "url", "url": "https://example.com"}
                    }
                },
                {"type": "code_interpreter_call", "id": "ci_1", "outputs": {"type": "logs", "logs": "ok"}},
                {"type": "local_shell_call", "id": "ls_1", "action": {"type": "exec", "command": "pwd"}},
                {"type": "shell_call", "call_id": "sh_1", "action": {"commands": "pwd"}},
                {"type": "shell_call_output", "call_id": "sh_1", "output": {"stdout": "ok", "stderr": ""}},
                {
                    "type": "tool_search_output",
                    "tools": {
                        "type": "mcp",
                        "server_label": "demo",
                        "allowed_tools": "read",
                        "require_approval": {"always": {"tool_names": "write"}}
                    }
                },
                {
                    "type": "mcp_list_tools",
                    "id": "mcp_1",
                    "server_label": "demo",
                    "tools": {
                        "name": "read",
                        "input_schema": {},
                        "annotations": {"readOnlyHint": true}
                    }
                },
                {"type": "function_call_output", "call_id": "fn_1", "output": {"type": "input_text", "text": "ok"}}
            ]
        }))
        .unwrap(),
    );

    let normalized = super::normalize_create_body(&route, body);
    let value: serde_json::Value = serde_json::from_slice(&normalized).unwrap();
    assert!(value["context_management"].is_array());
    assert!(value["include"].is_array());
    assert!(value["tools"].is_array());
    assert!(value["tools"][0]["allowed_callers"].is_array());
    assert!(value["tools"][0]["tools"].is_array());
    assert!(value["tools"][0]["tools"][0]["vector_store_ids"].is_array());
    assert!(value["tool_choice"]["tools"].is_array());

    let input = value["input"].as_array().unwrap();
    let output_text = &input[0]["content"][0];
    assert!(input[0]["content"].is_array());
    assert!(output_text["annotations"].is_array());
    assert!(output_text["logprobs"].is_array());
    assert!(output_text["logprobs"][0]["bytes"].is_array());
    assert!(output_text["logprobs"][0]["top_logprobs"].is_array());
    assert!(output_text["logprobs"][0]["top_logprobs"][0]["bytes"].is_array());
    assert!(input[1]["summary"].is_array());
    assert!(input[1]["content"].is_array());
    assert!(input[2]["queries"].is_array());
    assert!(input[2]["results"].is_array());
    assert!(input[3]["pending_safety_checks"].is_array());
    assert!(input[3]["action"]["keys"].is_array());
    assert!(input[3]["action"]["path"].is_array());
    assert!(input[3]["actions"].is_array());
    assert!(input[3]["actions"][0]["keys"].is_array());
    assert!(input[4]["acknowledged_safety_checks"].is_array());
    assert!(input[5]["action"]["queries"].is_array());
    assert!(input[5]["action"]["sources"].is_array());
    assert!(input[6]["outputs"].is_array());
    assert!(input[7]["action"]["command"].is_array());
    assert!(input[8]["action"]["commands"].is_array());
    assert!(input[9]["output"].is_array());
    assert!(input[10]["tools"].is_array());
    assert!(input[10]["tools"][0]["allowed_tools"].is_array());
    assert!(input[10]["tools"][0]["require_approval"]["always"]["tool_names"].is_array());
    assert!(input[11]["tools"].is_array());
    assert!(input[11]["tools"][0]["annotations"].is_object());
    assert!(input[12]["output"].is_array());

    let normalized_again = super::normalize_create_body(&route, normalized.clone());
    assert_eq!(normalized_again, normalized);
}

#[test]
fn wraps_a_single_structured_response_input_item() {
    let route = resolve_responses_route("POST", "/v1/responses").unwrap();
    let body = Bytes::from_static(
        br#"{"model":"gpt-test","input":{"type":"message","role":"user","content":{"type":"input_text","text":"hello"}}}"#,
    );
    let normalized = super::normalize_create_body(&route, body);
    let value: serde_json::Value = serde_json::from_slice(&normalized).unwrap();
    assert!(value["input"].is_array());
    assert!(value["input"][0]["content"].is_array());
}

#[test]
fn keeps_only_typed_output_text_annotations() {
    let route = resolve_responses_route("POST", "/v1/responses").unwrap();
    let body = Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "model": "gpt-test",
            "input": [{
                "type": "message",
                "role": "assistant",
                "content": [
                    {"type": "output_text", "text": "empty", "annotations": {}},
                    {
                        "type": "output_text",
                        "text": "mapped",
                        "annotations": {
                            "citation": {"type": "file_citation", "file_id": "file_1"},
                            "metadata": {"source": "client"}
                        }
                    },
                    {
                        "type": "output_text",
                        "text": "mixed",
                        "annotations": [
                            {},
                            {"type": "url_citation", "url": "https://example.com"},
                            "invalid"
                        ]
                    }
                ]
            }]
        }))
        .unwrap(),
    );

    let normalized = super::normalize_create_body(&route, body);
    let value: serde_json::Value = serde_json::from_slice(&normalized).unwrap();
    let content = value["input"][0]["content"].as_array().unwrap();
    assert_eq!(content[0]["annotations"], serde_json::json!([]));
    assert_eq!(
        content[1]["annotations"],
        serde_json::json!([{"type": "file_citation", "file_id": "file_1"}])
    );
    assert_eq!(
        content[2]["annotations"],
        serde_json::json!([{"type": "url_citation", "url": "https://example.com"}])
    );
}

#[test]
fn normalizes_reasoning_summary_and_content_discriminators() {
    let route = resolve_responses_route("POST", "/v1/responses").unwrap();
    let body = Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "model": "gpt-test",
            "input": [
                {
                    "type": "reasoning",
                    "id": "rs_1",
                    "summary": {"text": "summary"},
                    "content": "reasoning"
                },
                {
                    "type": "reasoning",
                    "id": "rs_2",
                    "summary": {},
                    "content": {
                        "first": {"text": "first"},
                        "invalid": {"metadata": true}
                    }
                },
                {
                    "type": "reasoning",
                    "id": "rs_3",
                    "summary": [
                        {"type": "summary_text", "text": "typed"},
                        {"text": "inferred"},
                        {},
                        {"type": "reasoning_text", "text": "wrong discriminator"}
                    ]
                }
            ]
        }))
        .unwrap(),
    );

    let normalized = super::normalize_create_body(&route, body);
    let value: serde_json::Value = serde_json::from_slice(&normalized).unwrap();
    assert_eq!(
        value["input"][0]["summary"],
        serde_json::json!([{"type": "summary_text", "text": "summary"}])
    );
    assert_eq!(
        value["input"][0]["content"],
        serde_json::json!([{"type": "reasoning_text", "text": "reasoning"}])
    );
    assert_eq!(value["input"][1]["summary"], serde_json::json!([]));
    assert_eq!(
        value["input"][1]["content"],
        serde_json::json!([{"type": "reasoning_text", "text": "first"}])
    );
    assert_eq!(
        value["input"][2]["summary"],
        serde_json::json!([
            {"type": "summary_text", "text": "typed"},
            {"type": "summary_text", "text": "inferred"}
        ])
    );
}

#[test]
fn normalizes_message_content_discriminators_and_omits_empty_parts() {
    let route = resolve_responses_route("POST", "/v1/responses").unwrap();
    let body = Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "model": "gpt-test",
            "input": [
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"text": "answer"}, {}]
                },
                {
                    "type": "message",
                    "role": "user",
                    "content": {
                        "text_part": {"text": "question"},
                        "image_part": {"image_url": "https://example.com/image.png"},
                        "invalid": {"metadata": true}
                    }
                }
            ]
        }))
        .unwrap(),
    );

    let normalized = super::normalize_create_body(&route, body);
    let value: serde_json::Value = serde_json::from_slice(&normalized).unwrap();
    assert_eq!(
        value["input"][0]["content"],
        serde_json::json!([{"type": "output_text", "text": "answer"}])
    );
    let user_content = value["input"][1]["content"].as_array().unwrap();
    assert_eq!(user_content.len(), 2);
    assert!(user_content.contains(
        &serde_json::json!({"type": "input_image", "image_url": "https://example.com/image.png"})
    ));
    assert!(user_content.contains(&serde_json::json!({"type": "input_text", "text": "question"})));
}

#[test]
fn normalizes_collections_for_compact_and_input_token_requests_without_adding_store() {
    for path in ["/v1/responses/compact", "/v1/responses/input_tokens"] {
        let route = resolve_responses_route("POST", path).unwrap();
        let body = Bytes::from_static(
            br#"{"model":"gpt-test","input":{"type":"message","role":"user","content":{"type":"input_text","text":"hello"}},"tools":{"type":"file_search","vector_store_ids":"vs_1"}}"#,
        );
        let normalized = super::normalize_create_body(&route, body);
        let value: serde_json::Value = serde_json::from_slice(&normalized).unwrap();
        assert!(value.get("store").is_none());
        assert!(value["input"].is_array());
        assert!(value["input"][0]["content"].is_array());
        assert!(value["tools"].is_array());
        assert!(value["tools"][0]["vector_store_ids"].is_array());
    }
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
        conversations: None,
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
