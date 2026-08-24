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
                {
                    "type": "text",
                    "text": "Describe this",
                    "prompt_cache_breakpoint": {"mode": "explicit"}
                },
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}}
            ]},
            {"role": "assistant", "content": null, "tool_calls": [{
                "id": "call_1",
                "type": "function",
                "function": {"name": "lookup", "arguments": "{\"id\":1}"}
            }]},
            {"role": "tool", "tool_call_id": "call_1", "content": [{
                "type": "text",
                "text": "found",
                "prompt_cache_breakpoint": {"mode": "explicit"}
            }]}
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
        "moderation": {
            "model": "omni-moderation-latest",
            "policy": {"input": {"mode": "score"}, "output": {"mode": "block"}}
        },
        "prompt_cache_options": {"mode": "implicit", "ttl": "30m"},
        "prompt_cache_retention": "in_memory",
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
                    {
                        "type": "input_text",
                        "text": "Describe this",
                        "prompt_cache_breakpoint": {"mode": "explicit"}
                    },
                    {"type": "input_image", "image_url": "data:image/png;base64,abc", "detail": "auto"}
                ]},
                {"type": "function_call", "call_id": "call_1", "name": "lookup", "arguments": "{\"id\":1}"},
                {"type": "function_call_output", "call_id": "call_1", "output": [{
                    "type": "input_text",
                    "text": "found",
                    "prompt_cache_breakpoint": {"mode": "explicit"}
                }]}
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
            "moderation": {
                "model": "omni-moderation-latest",
                "policy": {"input": {"mode": "score"}, "output": {"mode": "block"}}
            },
            "prompt_cache_options": {"mode": "implicit", "ttl": "30m"},
            "prompt_cache_retention": "in_memory",
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
fn translates_chat_file_parts_to_responses_input_files() {
    let request = serde_json::json!({
        "model": "gpt-test",
        "messages": [{
            "role": "user",
            "content": [
                {
                    "type": "file",
                    "file": {"file_id": "file_abc"}
                },
                {
                    "type": "file",
                    "file": {
                        "file_data": "data:application/pdf;base64,JVBERi0x",
                        "filename": "reference.pdf"
                    },
                    "prompt_cache_breakpoint": {"mode": "explicit"}
                }
            ]
        }]
    });

    let translated = translate_request(&serde_json::to_vec(&request).unwrap()).unwrap();
    let body: Value = serde_json::from_slice(&translated.body).unwrap();
    assert_eq!(
        body["input"][0]["content"],
        serde_json::json!([
            {"type": "input_file", "file_id": "file_abc"},
            {
                "type": "input_file",
                "file_data": "data:application/pdf;base64,JVBERi0x",
                "filename": "reference.pdf",
                "prompt_cache_breakpoint": {"mode": "explicit"}
            }
        ])
    );
}

#[test]
fn preserves_chat_participant_names_in_message_content() {
    let request = serde_json::json!({
        "model": "gpt-test",
        "messages": [
            {"role": "developer", "name": "policy", "content": "Follow policy"},
            {"role": "system", "name": "router", "content": "Route requests"},
            {"role": "user", "name": "Alice\nAdmin", "content": [
                {"type": "text", "text": "Hello"}
            ]},
            {"role": "assistant", "name": "worker", "tool_calls": [{
                "id": "call_1",
                "type": "function",
                "function": {"name": "lookup", "arguments": "{}"}
            }]}
        ]
    });

    let translated = translate_request(&serde_json::to_vec(&request).unwrap()).unwrap();
    let body: Value = serde_json::from_slice(&translated.body).unwrap();
    assert_eq!(
        body["input"],
        serde_json::json!([
            {"type": "message", "role": "developer", "content": [
                {"type": "input_text", "text": "[participant name=\"policy\"]\n"},
                {"type": "input_text", "text": "Follow policy"}
            ]},
            {"type": "message", "role": "system", "content": [
                {"type": "input_text", "text": "[participant name=\"router\"]\n"},
                {"type": "input_text", "text": "Route requests"}
            ]},
            {"type": "message", "role": "user", "content": [
                {"type": "input_text", "text": "[participant name=\"Alice\\nAdmin\"]\n"},
                {"type": "input_text", "text": "Hello"}
            ]},
            {"type": "message", "role": "assistant", "content": [
                {"type": "output_text", "text": "[participant name=\"worker\"]\n"}
            ]},
            {"type": "function_call", "call_id": "call_1", "name": "lookup", "arguments": "{}"}
        ])
    );
}

#[test]
fn rejects_non_string_chat_participant_names() {
    let request = serde_json::json!({
        "model": "gpt-test",
        "messages": [{"role": "user", "name": ["Alice"], "content": "Hello"}]
    });

    let error = translate_request(&serde_json::to_vec(&request).unwrap()).unwrap_err();
    assert_eq!(error.param.as_deref(), Some("messages[0].name"));
}

#[test]
fn translates_custom_tools_calls_outputs_and_choices() {
    let request = serde_json::json!({
        "model": "gpt-test",
        "messages": [
            {"role": "assistant", "tool_calls": [{
                "id": "call_custom",
                "type": "custom",
                "custom": {"name": "shell", "input": "pwd"}
            }]},
            {"role": "tool", "tool_call_id": "call_custom", "content": "work/"}
        ],
        "tools": [{
            "type": "custom",
            "custom": {
                "name": "shell",
                "description": "Run a command",
                "format": {
                    "type": "grammar",
                    "grammar": {"syntax": "lark", "definition": "start: /.+/"}
                }
            }
        }],
        "tool_choice": {"type": "custom", "custom": {"name": "shell"}}
    });

    let translated = translate_request(&serde_json::to_vec(&request).unwrap()).unwrap();
    let body: Value = serde_json::from_slice(&translated.body).unwrap();
    assert_eq!(
        body["input"],
        serde_json::json!([
            {"type": "custom_tool_call", "call_id": "call_custom", "name": "shell", "input": "pwd"},
            {"type": "custom_tool_call_output", "call_id": "call_custom", "output": "work/"}
        ])
    );
    assert_eq!(
        body["tools"],
        serde_json::json!([{
            "type": "custom",
            "name": "shell",
            "description": "Run a command",
            "format": {"type": "grammar", "syntax": "lark", "definition": "start: /.+/"}
        }])
    );
    assert_eq!(
        body["tool_choice"],
        serde_json::json!({"type": "custom", "name": "shell"})
    );
}

#[test]
fn translates_allowed_tools_choice_as_a_collection_of_flattened_tools() {
    let request = serde_json::json!({
        "model": "gpt-test",
        "messages": [],
        "tool_choice": {
            "type": "allowed_tools",
            "allowed_tools": {
                "mode": "required",
                "tools": [
                    {"type": "function", "function": {"name": "lookup"}},
                    {"type": "custom", "custom": {"name": "shell"}}
                ]
            }
        }
    });

    let translated = translate_request(&serde_json::to_vec(&request).unwrap()).unwrap();
    let body: Value = serde_json::from_slice(&translated.body).unwrap();
    assert_eq!(
        body["tool_choice"],
        serde_json::json!({
            "type": "allowed_tools",
            "mode": "required",
            "tools": [
                {"type": "function", "name": "lookup"},
                {"type": "custom", "name": "shell"}
            ]
        })
    );
}

#[test]
fn rejects_malformed_chat_file_parts_with_a_precise_parameter() {
    let request = serde_json::json!({
        "model": "gpt-test",
        "messages": [{
            "role": "user",
            "content": [{"type": "file", "file": {"filename": "empty.pdf"}}]
        }]
    });

    let error = translate_request(&serde_json::to_vec(&request).unwrap()).unwrap_err();
    assert_eq!(error.param.as_deref(), Some("messages[0].content[0].file"));
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

    let malformed_n = serde_json::json!({
        "model": "gpt-test",
        "messages": [],
        "n": "1"
    });
    let err = translate_request(&serde_json::to_vec(&malformed_n).unwrap()).unwrap_err();
    assert_eq!(err.param.as_deref(), Some("n"));

    let unknown = serde_json::json!({
        "model": "gpt-test",
        "messages": [],
        "unknown": true
    });
    let err = translate_request(&serde_json::to_vec(&unknown).unwrap()).unwrap_err();
    assert_eq!(err.param.as_deref(), Some("unknown"));

    let non_streaming_options = serde_json::json!({
        "model": "gpt-test",
        "messages": [],
        "stream": false,
        "stream_options": {"include_usage": true}
    });
    let err = translate_request(&serde_json::to_vec(&non_streaming_options).unwrap()).unwrap_err();
    assert_eq!(err.param.as_deref(), Some("stream_options"));

    let empty_assistant = serde_json::json!({
        "model": "gpt-test",
        "messages": [{"role": "assistant", "content": null, "refusal": null}]
    });
    let err = translate_request(&serde_json::to_vec(&empty_assistant).unwrap()).unwrap_err();
    assert_eq!(err.param.as_deref(), Some("messages[0].content"));

    let conflicting_limits = serde_json::json!({
        "model": "gpt-test",
        "messages": [],
        "max_tokens": 10,
        "max_completion_tokens": 20
    });
    let err = translate_request(&serde_json::to_vec(&conflicting_limits).unwrap()).unwrap_err();
    assert_eq!(err.param.as_deref(), Some("max_tokens"));
}

#[test]
fn preserves_both_current_and_deprecated_function_tool_arrays() {
    let request = serde_json::json!({
        "model": "gpt-test",
        "messages": [],
        "tools": [{"type": "custom", "custom": {"name": "shell"}}],
        "functions": [{"name": "lookup", "parameters": {"type": "object"}}]
    });

    let translated = translate_request(&serde_json::to_vec(&request).unwrap()).unwrap();
    let body: Value = serde_json::from_slice(&translated.body).unwrap();
    assert_eq!(
        body["tools"],
        serde_json::json!([
            {"type": "custom", "name": "shell"},
            {"type": "function", "name": "lookup", "parameters": {"type": "object"}}
        ])
    );
}

#[test]
fn enforces_role_specific_chat_content_and_translates_refusals() {
    let invalid = serde_json::json!({
        "model": "gpt-test",
        "messages": [{
            "role": "system",
            "content": [{"type": "image_url", "image_url": {"url": "https://example.com/a.png"}}]
        }]
    });
    assert!(translate_request(&serde_json::to_vec(&invalid).unwrap()).is_err());

    let request = serde_json::json!({
        "model": "gpt-test",
        "messages": [{
            "role": "assistant",
            "content": [{"type": "refusal", "refusal": "cannot comply"}]
        }]
    });
    let translated = translate_request(&serde_json::to_vec(&request).unwrap()).unwrap();
    let body: Value = serde_json::from_slice(&translated.body).unwrap();
    assert_eq!(
        body["input"],
        serde_json::json!([{
            "type": "message",
            "role": "assistant",
            "content": [{"type": "refusal", "refusal": "cannot comply"}]
        }])
    );
}

#[test]
fn converts_terminal_response_to_chat_completion() {
    let response = serde_json::json!({
        "id": "resp_abc",
        "created_at": 123,
        "model": "gpt-test-2026-08-23",
        "status": "completed",
        "moderation": {"input": {"flagged": false}, "output": {"flagged": false}},
        "output": [
            {"type": "message", "role": "assistant", "content": [
                {"type": "output_text", "text": "Calling "},
                {"type": "output_text", "text": "a tool"}
            ]},
            {"type": "function_call", "call_id": "call_1", "name": "lookup", "arguments": "{\"id\":1}"},
            {"type": "custom_tool_call", "call_id": "call_2", "name": "shell", "input": "pwd"}
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
            "moderation": {"input": {"flagged": false}, "output": {"flagged": false}},
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Calling a tool",
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "lookup", "arguments": "{\"id\":1}"}
                    }, {
                        "id": "call_2",
                        "type": "custom",
                        "custom": {"name": "shell", "input": "pwd"}
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
fn translates_custom_tool_stream_events_to_chat_chunks() {
    let mut translator = StreamTranslator::new("requested-model".to_string(), false);
    let events = [
        serde_json::json!({
            "type": "response.created",
            "response": {"id": "resp_custom", "created_at": 456, "model": "gpt-test"}
        }),
        serde_json::json!({
            "type": "response.output_item.added",
            "output_index": 3,
            "item": {"type": "custom_tool_call", "call_id": "call_custom", "name": "shell"}
        }),
        serde_json::json!({
            "type": "response.custom_tool_call_input.delta",
            "output_index": 3,
            "delta": "pwd"
        }),
        serde_json::json!({
            "type": "response.completed",
            "response": {
                "id": "resp_custom",
                "created_at": 456,
                "model": "gpt-test",
                "output": [{"type": "custom_tool_call"}]
            }
        }),
    ];
    let frames = events
        .iter()
        .flat_map(|event| translator.translate_event(event))
        .collect::<Vec<Bytes>>();

    assert_eq!(frames.len(), 5);
    let chunks = frames[..4].iter().map(parse_sse_json).collect::<Vec<_>>();
    assert_eq!(
        chunks[1]["choices"][0]["delta"]["tool_calls"][0],
        serde_json::json!({
            "index": 0,
            "id": "call_custom",
            "type": "custom",
            "custom": {"name": "shell", "input": ""}
        })
    );
    assert_eq!(
        chunks[2]["choices"][0]["delta"]["tool_calls"][0],
        serde_json::json!({"index": 0, "custom": {"input": "pwd"}})
    );
    assert_eq!(chunks[3]["choices"][0]["finish_reason"], "tool_calls");
    assert_eq!(frames[4], Bytes::from_static(b"data: [DONE]\n\n"));
}

#[test]
fn translates_responses_events_to_chat_completion_chunks() {
    let mut translator = StreamTranslator::new("requested-model".to_string(), true);
    let events = [
        serde_json::json!({
            "type": "response.created",
            "obfuscation": "pad",
            "response": {
                "id": "resp_abc",
                "created_at": 123,
                "model": "gpt-test",
                "service_tier": "priority",
                "system_fingerprint": "fp_test"
            }
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
    assert_eq!(chunks[0]["obfuscation"], "pad");
    assert_eq!(chunks[0]["service_tier"], "priority");
    assert_eq!(chunks[0]["system_fingerprint"], "fp_test");
    assert!(chunks[0]["usage"].is_null());
    assert_eq!(chunks[1]["choices"][0]["delta"]["content"], "hello");
    assert_eq!(chunks[2]["choices"][0]["finish_reason"], "stop");
    assert_eq!(chunks[2]["service_tier"], "priority");
    assert_eq!(chunks[2]["system_fingerprint"], "fp_test");
    assert_eq!(chunks[3]["choices"], serde_json::json!([]));
    assert_eq!(chunks[3]["usage"]["total_tokens"], 3);
    assert_eq!(chunks[3]["service_tier"], "priority");
    assert_eq!(chunks[3]["system_fingerprint"], "fp_test");
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
