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
fn normalizes_backend_singletons_for_public_response_collections() {
    let mut item = serde_json::json!({
        "type": "message",
        "role": "assistant",
        "content": {
            "type": "output_text",
            "text": "answer",
            "annotations": {"type": "file_citation", "file_id": "file_1"},
            "logprobs": {"token": "a", "bytes": 97}
        }
    });
    super::normalize_response_item_collections(&mut item);
    assert!(item["content"].is_array());
    assert!(item["content"][0]["annotations"].is_array());
    assert!(item["content"][0]["logprobs"].is_array());
    assert!(item["content"][0]["logprobs"][0]["bytes"].is_array());

    let mut output = serde_json::json!({
        "type": "custom_tool_call_output",
        "call_id": "custom_1",
        "output": {"image_url": "https://example.com/tool.png"}
    });
    super::normalize_response_item_collections(&mut output);
    assert_eq!(output["output"][0]["type"], "input_image");
    assert_eq!(output["output"][0]["detail"], "auto");

    let mut nullable = serde_json::json!({
        "type": "additional_tools",
        "tools": [
            {"type": "mcp", "allowed_callers": null, "allowed_tools": null},
            {"type": "web_search_2025_08_26", "filters": {"allowed_domains": null}}
        ]
    });
    super::normalize_response_item_collections(&mut nullable);
    assert!(nullable["tools"][0]["allowed_callers"].is_null());
    assert!(nullable["tools"][0]["allowed_tools"].is_null());
    assert!(nullable["tools"][1]["filters"]["allowed_domains"].is_null());
}

#[test]
fn rejects_singletons_for_array_only_response_fields() {
    let route = resolve_responses_route("POST", "/v1/responses").unwrap();
    let cases = [
        serde_json::json!({"input": {"role": "user", "content": "hello"}}),
        serde_json::json!({"input": [{"role": "user", "content": {"type": "input_text", "text": "hello"}}]}),
        serde_json::json!({"input": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "hello", "annotations": [], "logprobs": [{"token": "h", "bytes": [104], "top_logprobs": [{"token": "h", "bytes": 104, "logprob": 0.0}], "logprob": 0.0}]}]}]}),
        serde_json::json!({"input": [{"type": "reasoning", "summary": [], "content": null}]}),
        serde_json::json!({"include": "reasoning.encrypted_content"}),
        serde_json::json!({"context_management": {"type": "compaction"}}),
        serde_json::json!({"instructions": [{"role": "developer", "content": "be concise"}]}),
        serde_json::json!({"tools": null}),
        serde_json::json!({"tools": {"type": "function", "name": "lookup"}}),
        serde_json::json!({"tool_choice": {"type": "allowed_tools", "mode": "auto", "tools": {"type": "function", "name": "lookup"}}}),
        serde_json::json!({"tools": [{"type": "file_search", "vector_store_ids": "vs_1"}]}),
        serde_json::json!({"tools": [{"type": "file_search", "vector_store_ids": ["vs_1"], "filters": {"type": "and", "filters": {"type": "eq", "key": "kind", "value": "doc"}}}]}),
        serde_json::json!({"tools": [{"type": "web_search_2025_08_26", "filters": {"allowed_domains": "example.com"}}]}),
        serde_json::json!({"tools": [{"type": "web_search_preview", "search_content_types": null}]}),
    ];
    for value in cases {
        let body = serde_json::to_vec(&value).unwrap();
        assert!(
            super::validate_response_request_collections(&route, &body).is_err(),
            "accepted invalid request: {value}"
        );
    }
}

#[test]
fn preserves_null_only_for_publicly_nullable_response_collections() {
    let create = resolve_responses_route("POST", "/v1/responses").unwrap();
    let create_body = serde_json::json!({
        "context_management": null,
        "include": null,
        "tools": [
            {"type": "function", "name": "lookup", "allowed_callers": null},
            {"type": "web_search_2025_08_26", "filters": {"allowed_domains": null}}
        ],
        "input": [
            {"type": "file_search_call", "queries": [], "results": null},
            {"type": "code_interpreter_call", "outputs": null}
        ]
    });
    assert!(
        super::validate_response_request_collections(
            &create,
            &serde_json::to_vec(&create_body).unwrap()
        )
        .is_ok()
    );

    let input_tokens = resolve_responses_route("POST", "/v1/responses/input_tokens").unwrap();
    let input_tokens_body = serde_json::json!({"input": null, "tools": null});
    assert!(
        super::validate_response_request_collections(
            &input_tokens,
            &serde_json::to_vec(&input_tokens_body).unwrap()
        )
        .is_ok()
    );
}

#[test]
fn keeps_only_typed_output_text_annotations() {
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

    let mut value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    super::normalize_response_item_collections(&mut value["input"][0]);
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

    let mut value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    for item in value["input"].as_array_mut().unwrap() {
        super::normalize_response_item_collections(item);
    }
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

    let mut value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    for item in value["input"].as_array_mut().unwrap() {
        super::normalize_response_item_collections(item);
    }
    assert_eq!(
        value["input"][0]["content"],
        serde_json::json!([{"type": "output_text", "text": "answer"}])
    );
    let user_content = value["input"][1]["content"].as_array().unwrap();
    assert_eq!(user_content.len(), 2);
    assert!(user_content.contains(
        &serde_json::json!({"type": "input_image", "image_url": "https://example.com/image.png", "detail": "auto"})
    ));
    assert!(user_content.contains(&serde_json::json!({"type": "input_text", "text": "question"})));
}

#[test]
fn normalizes_collections_for_compact_and_input_token_requests_without_adding_store() {
    for path in ["/v1/responses/compact", "/v1/responses/input_tokens"] {
        let route = resolve_responses_route("POST", path).unwrap();
        let body = Bytes::from_static(
            br#"{"model":"gpt-test","input":"hello","tools":[{"type":"file_search","vector_store_ids":["vs_1"]}]}"#,
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
