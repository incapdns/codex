use clap::Parser;
use pretty_assertions::assert_eq;
use std::net::IpAddr;

use super::Args;
use super::ProxyAuth;

#[test]
fn parses_chatgpt_auth_with_standard_config_overrides() {
    let args = Args::try_parse_from([
        "responses-api-proxy",
        "--auth",
        "chatgpt",
        "--port",
        "8080",
        "--listen-host",
        "0.0.0.0",
        "--strict-config",
        "--chat-completions-compat=false",
        "--conversations-compat=false",
        "--conversation-store",
        "/tmp/conversations.json",
        "--conversation-compact-after-items",
        "40",
        "-c",
        "openai_base_url=\"https://example.test/backend-api/codex\"",
    ])
    .expect("arguments should parse");

    assert_eq!(
        (
            args.auth,
            args.port,
            args.listen_host,
            args.strict_config,
            args.chat_completions_compat,
            args.conversations_compat,
            args.conversation_store,
            args.conversation_compact_after_items,
            args.config_overrides.raw_overrides,
        ),
        (
            ProxyAuth::Chatgpt,
            Some(8080),
            Some(IpAddr::from([0, 0, 0, 0])),
            true,
            Some(false),
            Some(false),
            Some("/tmp/conversations.json".into()),
            Some(40),
            vec!["openai_base_url=\"https://example.test/backend-api/codex\"".to_string()],
        )
    );
}

#[test]
fn stdin_auth_remains_the_default() {
    let args =
        Args::try_parse_from(["responses-api-proxy"]).expect("default arguments should parse");

    assert_eq!(
        (
            args.auth,
            args.upstream_url,
            args.listen_host,
            args.chat_completions_compat,
            args.conversations_compat,
            args.conversation_store,
            args.conversation_compact_after_items,
        ),
        (ProxyAuth::Stdin, None, None, None, None, None, None)
    );
}

#[test]
fn listen_host_requires_an_ip_address() {
    let ipv6 = Args::try_parse_from(["responses-api-proxy", "--listen-host", "::1"])
        .expect("IPv6 listen address should parse");
    assert_eq!(ipv6.listen_host, Some("::1".parse().unwrap()));

    assert!(
        Args::try_parse_from(["responses-api-proxy", "--listen-host", "localhost"]).is_err(),
        "hostnames should be rejected so the socket bind address is explicit"
    );
}
