use axum::body::Bytes;
use pretty_assertions::assert_eq;
use serde_json::Value;

use super::StreamTranslator;
use super::completion_from_response;
use super::translate_request;

#[test]
fn translates_chat_messages_tools_and_public_shorthand() {
    let request = serde_json::json!({
        "model": "gpt-test",
        "messages": [
            {"role": "system", "content": "Be concise"},
            {"role": "user", "content": [
                {"type": "text", "text": "Describe this"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc", "detail": "low"}}
            ]},
            {"role": "assistant", "content": null, "tool_calls": [{
                "id": "call_1",
                "type": "function",
                "function": {"name": "lookup", "arguments": "{\"id\":1}"}
            }]},
            {"role": "tool", "tool_call_id": "call_1", "content": "found"}
        ],
        "tools": [{
            "type": "function",
            "function": {
                "name": "lookup",
                "description": "Find a row",
                "parameters": {"type": "object"},
                "strict": true
            }
        }],
        "tool_choice": {"type": "function", "function": {"name": "lookup"}},
        "max_completion_tokens": 200,
        "reasoning_effort": "low",
        "response_format": {
            "type": "json_schema",
            "json_schema": {"name": "answer", "schema": {"type": "object"}, "strict": true}
        },
        "stream": false
    });

    let translated = translate_request(&serde_json::to_vec(&request).unwrap()).unwrap();
    assert!(!translated.client_stream);
    assert_eq!(translated.model, "gpt-test");
    assert_eq!(
        serde_json::from_slice::<Value>(&translated.body).unwrap(),
        serde_json::json!({
            "model": "gpt-test",
            "input": [
                {"type": "message", "role": "system", "content": [{"type": "input_text", "text": "Be concise"}]},
                {"type": "message", "role": "user", "content": [
                    {"type": "input_text", "text": "Describe this"},
                    {"type": "input_image", "image_url": "data:image/png;base64,abc", "detail": "low"}
                ]},
                {"type": "function_call", "call_id": "call_1", "name": "lookup", "arguments": "{\"id\":1}"},
                {"type": "function_call_output", "call_id": "call_1", "output": "found"}
            ],
            "store": false,
            "stream": true,
            "tools": [{
                "type": "function",
                "name": "lookup",
                "description": "Find a row",
                "parameters": {"type": "object"},
                "strict": true
            }],
            "tool_choice": {"type": "function", "name": "lookup"},
            "max_output_tokens": 200,
            "reasoning": {"effort": "low"},
            "text": {"format": {
                "type": "json_schema",
                "name": "answer",
                "schema": {"type": "object"},
                "strict": true
            }}
        })
    );
}

#[test]
fn rejects_backend_incompatible_storage_and_multiple_choices() {
    let store = serde_json::json!({
        "model": "gpt-test",
        "messages": [],
        "store": true
    });
    let err = translate_request(&serde_json::to_vec(&store).unwrap()).unwrap_err();
    assert_eq!(err.param.as_deref(), Some("store"));

    let multiple = serde_json::json!({
        "model": "gpt-test",
        "messages": [],
        "n": 2
    });
    let err = translate_request(&serde_json::to_vec(&multiple).unwrap()).unwrap_err();
    assert_eq!(err.param.as_deref(), Some("n"));
}

#[test]
fn converts_terminal_response_to_chat_completion() {
    let response = serde_json::json!({
        "id": "resp_abc",
        "created_at": 123,
        "model": "gpt-test-2026-08-23",
        "status": "completed",
        "output": [
            {"type": "message", "role": "assistant", "content": [
                {"type": "output_text", "text": "Calling "},
                {"type": "output_text", "text": "a tool"}
            ]},
            {"type": "function_call", "call_id": "call_1", "name": "lookup", "arguments": "{\"id\":1}"}
        ],
        "usage": {
            "input_tokens": 10,
            "output_tokens": 4,
            "total_tokens": 14,
            "input_tokens_details": {"cached_tokens": 3},
            "output_tokens_details": {"reasoning_tokens": 2}
        }
    });

    assert_eq!(
        completion_from_response(&response, "requested-model"),
        serde_json::json!({
            "id": "chatcmpl-abc",
            "object": "chat.completion",
            "created": 123,
            "model": "gpt-test-2026-08-23",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Calling a tool",
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "lookup", "arguments": "{\"id\":1}"}
                    }]
                },
                "logprobs": null,
                "finish_reason": "tool_calls"
            }],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 4,
                "total_tokens": 14,
                "prompt_tokens_details": {"cached_tokens": 3},
                "completion_tokens_details": {"reasoning_tokens": 2}
            }
        })
    );
}

#[test]
fn translates_responses_events_to_chat_completion_chunks() {
    let mut translator = StreamTranslator::new("requested-model".to_string(), true);
    let events = [
        serde_json::json!({
            "type": "response.created",
            "response": {"id": "resp_abc", "created_at": 123, "model": "gpt-test"}
        }),
        serde_json::json!({"type": "response.output_text.delta", "delta": "hello"}),
        serde_json::json!({
            "type": "response.completed",
            "response": {
                "id": "resp_abc",
                "created_at": 123,
                "model": "gpt-test",
                "output": [],
                "usage": {"input_tokens": 2, "output_tokens": 1, "total_tokens": 3}
            }
        }),
    ];
    let frames = events
        .iter()
        .flat_map(|event| translator.translate_event(event))
        .collect::<Vec<Bytes>>();

    assert_eq!(frames.len(), 5);
    let chunks = frames[..4].iter().map(parse_sse_json).collect::<Vec<_>>();
    assert_eq!(chunks[0]["id"], "chatcmpl-abc");
    assert_eq!(chunks[0]["choices"][0]["delta"]["role"], "assistant");
    assert_eq!(chunks[1]["choices"][0]["delta"]["content"], "hello");
    assert_eq!(chunks[2]["choices"][0]["finish_reason"], "stop");
    assert_eq!(chunks[3]["choices"], serde_json::json!([]));
    assert_eq!(chunks[3]["usage"]["total_tokens"], 3);
    assert_eq!(frames[4], Bytes::from_static(b"data: [DONE]\n\n"));
}

fn parse_sse_json(frame: &Bytes) -> Value {
    let frame = std::str::from_utf8(frame).unwrap();
    let data = frame
        .strip_prefix("data: ")
        .and_then(|frame| frame.strip_suffix("\n\n"))
        .unwrap();
    serde_json::from_str(data).unwrap()
}
