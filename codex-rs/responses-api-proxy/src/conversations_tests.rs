use axum::body::to_bytes;
use codex_http_client::HttpClient;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use http::HeaderMap;
use pretty_assertions::assert_eq;
use serde_json::Value;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::body_json;
use wiremock::matchers::method;
use wiremock::matchers::path;

use super::ConversationStore;
use super::ListOrder;
use super::ListQuery;
use crate::chatgpt::ChatgptState;
use crate::chatgpt::forward_request;
use crate::routes::resolve_responses_route;

#[tokio::test]
async fn persists_complete_conversation_and_items_crud_contract() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("conversations.json");
    let store = ConversationStore::new_for_testing(path.clone(), 80).await;

    let created = store
        .create_conversation(
            br#"{
                "metadata":{"topic":"demo"},
                "items":[{"role":"user","content":"hello"}]
            }"#,
        )
        .await
        .unwrap();
    let conversation_id = created["id"].as_str().unwrap();
    assert!(conversation_id.starts_with("conv_"));
    assert_eq!(created["object"], "conversation");
    assert_eq!(created["metadata"], serde_json::json!({"topic": "demo"}));

    let updated = store
        .update_conversation(conversation_id, br#"{"metadata":{"topic":"updated"}}"#)
        .await
        .unwrap();
    assert_eq!(updated["metadata"], serde_json::json!({"topic": "updated"}));

    let added = store
        .create_items(
            conversation_id,
            br#"{"items":[{"type":"message","role":"user","content":"second"}]}"#,
        )
        .await
        .unwrap();
    let second_id = added["data"][0]["id"].as_str().unwrap().to_string();
    assert_eq!(added["data"][0]["status"], "completed");
    assert_eq!(added["data"][0]["content"][0]["type"], "input_text");

    let ascending = store
        .list_items(
            conversation_id,
            ListQuery {
                after: None,
                limit: 100,
                order: ListOrder::Asc,
            },
        )
        .await
        .unwrap();
    assert_eq!(ascending["data"].as_array().unwrap().len(), 2);
    assert_eq!(ascending["last_id"], second_id);

    let retrieved = store
        .retrieve_item(conversation_id, &second_id)
        .await
        .unwrap();
    assert_eq!(retrieved["id"], second_id);
    let conversation = store
        .delete_item(conversation_id, &second_id)
        .await
        .unwrap();
    assert_eq!(conversation["object"], "conversation");

    drop(store);
    let reloaded = ConversationStore::load(path, 80).await.unwrap();
    let remaining = reloaded
        .list_items(
            conversation_id,
            ListQuery {
                after: None,
                limit: 20,
                order: ListOrder::Desc,
            },
        )
        .await
        .unwrap();
    assert_eq!(remaining["data"].as_array().unwrap().len(), 1);

    let deleted = reloaded.delete_conversation(conversation_id).await.unwrap();
    assert_eq!(
        deleted,
        serde_json::json!({
            "id": conversation_id,
            "object": "conversation.deleted",
            "deleted": true,
        })
    );
    assert!(
        reloaded
            .retrieve_conversation(conversation_id)
            .await
            .is_err()
    );
    let data = reloaded.data.lock().await;
    assert_eq!(data.conversations[conversation_id].items.len(), 1);
}

#[tokio::test]
async fn prepends_history_and_persists_response_items() {
    let directory = TempDir::new().unwrap();
    let store = ConversationStore::new_for_testing(directory.path().join("store.json"), 80).await;
    let conversation = store
        .create_conversation(br#"{"items":[{"role":"user","content":"first"}]}"#)
        .await
        .unwrap();
    let conversation_id = conversation["id"].as_str().unwrap();
    let request = serde_json::json!({
        "model": "gpt-test",
        "conversation": {"id": conversation_id},
        "input": "second",
        "stream": false,
        "store": true
    });
    let prepared = store
        .begin_response(&serde_json::to_vec(&request).unwrap())
        .await
        .unwrap()
        .unwrap();
    let body = store.upstream_body(&prepared).unwrap();
    let upstream: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(upstream["store"], false);
    assert!(upstream.get("conversation").is_none());
    assert_eq!(upstream["input"].as_array().unwrap().len(), 2);
    assert!(upstream["input"][0].get("id").is_none());
    assert_eq!(
        upstream["include"],
        serde_json::json!(["reasoning.encrypted_content"])
    );

    let conflict = store
        .begin_response(&serde_json::to_vec(&request).unwrap())
        .await
        .unwrap_err();
    assert_eq!(conflict.status, http::StatusCode::CONFLICT);

    store
        .complete_response(
            &prepared,
            vec![serde_json::json!({
                "id": "msg_upstream",
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "answer", "annotations": []}]
            })],
        )
        .await
        .unwrap();
    let items = store
        .list_items(
            conversation_id,
            ListQuery {
                after: None,
                limit: 20,
                order: ListOrder::Asc,
            },
        )
        .await
        .unwrap();
    assert_eq!(items["data"].as_array().unwrap().len(), 3);
    assert_eq!(items["data"][2]["id"], "msg_upstream");
}

#[tokio::test]
async fn compacts_supported_models_and_uses_checkpoint_as_context() {
    let directory = TempDir::new().unwrap();
    let store = ConversationStore::new_for_testing(directory.path().join("store.json"), 1).await;
    let conversation = store
        .create_conversation(br#"{"items":[{"role":"user","content":"old"}]}"#)
        .await
        .unwrap();
    let conversation_id = conversation["id"].as_str().unwrap();
    let request = serde_json::json!({
        "model": "gpt-test",
        "conversation": conversation_id,
        "input": "new"
    });
    let mut prepared = store
        .begin_response(&serde_json::to_vec(&request).unwrap())
        .await
        .unwrap()
        .unwrap();

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/responses/compact"))
        .and(body_json(serde_json::json!({
            "model": "gpt-test",
            "input": [{
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "old"}]
            }]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": "resp_compact",
            "object": "response.compaction",
            "created_at": 123,
            "output": [{"id": "cmp_1", "type": "compaction", "encrypted_content": "opaque"}]
        })))
        .mount(&server)
        .await;
    let state = test_state(&server, Some(store.clone()));
    store
        .maybe_compact(&state, &HeaderMap::new(), &mut prepared)
        .await;
    let body: Value = serde_json::from_slice(&store.upstream_body(&prepared).unwrap()).unwrap();
    assert_eq!(body["input"].as_array().unwrap().len(), 2);
    assert_eq!(body["input"][0]["type"], "compaction");
    assert_eq!(body["input"][1]["content"][0]["text"], "new");
    let data = store.data.lock().await;
    assert!(data.conversations[conversation_id].checkpoint.is_some());
}

#[tokio::test]
async fn rewrites_streaming_response_and_persists_completed_output() {
    let directory = TempDir::new().unwrap();
    let store = ConversationStore::new_for_testing(directory.path().join("store.json"), 80).await;
    let conversation = store.create_conversation(b"").await.unwrap();
    let conversation_id = conversation["id"].as_str().unwrap();
    let request = serde_json::json!({
        "model": "gpt-test",
        "conversation": conversation_id,
        "input": "hello",
        "stream": true
    });
    let prepared = store
        .begin_response(&serde_json::to_vec(&request).unwrap())
        .await
        .unwrap()
        .unwrap();

    let server = MockServer::start().await;
    let response_object = serde_json::json!({
        "id": "resp_1",
        "object": "response",
        "created_at": 123,
        "status": "completed",
        "model": "gpt-test",
        "output": [{
            "id": "msg_answer",
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [{"type": "output_text", "text": "answer", "annotations": []}]
        }]
    });
    let sse = format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        serde_json::json!({
            "type": "response.created",
            "response": {
                "id": "resp_1",
                "object": "response",
                "created_at": 123,
                "status": "in_progress",
                "model": "gpt-test",
                "output": []
            }
        }),
        serde_json::json!({"type": "response.completed", "response": response_object})
    );
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(sse, "text/event-stream"))
        .mount(&server)
        .await;
    let state = test_state(&server, Some(store.clone()));
    let route = resolve_responses_route("POST", "/v1/responses").unwrap();
    let upstream = forward_request(
        &state,
        &route,
        HeaderMap::new(),
        store.upstream_body(&prepared).unwrap(),
    )
    .await
    .unwrap();
    let response = std::sync::Arc::new(store.clone())
        .adapt_upstream_response(upstream, prepared, None)
        .await;
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains(&format!(r#""conversation":{{"id":"{conversation_id}"}}"#)));
    assert!(body.ends_with("data: [DONE]\n\n"));

    let items = store
        .list_items(
            conversation_id,
            ListQuery {
                after: None,
                limit: 20,
                order: ListOrder::Asc,
            },
        )
        .await
        .unwrap();
    assert_eq!(items["data"].as_array().unwrap().len(), 2);
    assert_eq!(items["data"][1]["id"], "msg_answer");
}

#[test]
fn route_parser_accepts_the_complete_official_resource() {
    let cases = [
        ("POST", "/v1/conversations"),
        ("GET", "/v1/conversations/conv_1"),
        ("POST", "/v1/conversations/conv_1"),
        ("DELETE", "/v1/conversations/conv_1"),
        (
            "POST",
            "/v1/conversations/conv_1/items?include=reasoning.encrypted_content",
        ),
        (
            "GET",
            "/v1/conversations/conv_1/items?after=msg_1&limit=10&order=asc",
        ),
        (
            "GET",
            "/v1/conversations/conv_1/items/msg_1?include[]=message.input_image.image_url",
        ),
        ("DELETE", "/v1/conversations/conv_1/items/msg_1"),
    ];
    for (method, uri) in cases {
        let method = http::Method::from_bytes(method.as_bytes()).unwrap();
        let uri = uri.parse().unwrap();
        assert!(
            super::resolve_route(&method, &uri).is_ok(),
            "{method} {uri}"
        );
    }
}

fn test_state(server: &MockServer, conversations: Option<ConversationStore>) -> ChatgptState {
    ChatgptState {
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
        conversations: conversations.map(std::sync::Arc::new),
        shutdown: CancellationToken::new(),
    }
}
