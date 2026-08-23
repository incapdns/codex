use clap::Parser;
use pretty_assertions::assert_eq;

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
        "--strict-config",
        "--chat-completions-compat=false",
        "-c",
        "openai_base_url=\"https://example.test/backend-api/codex\"",
    ])
    .expect("arguments should parse");

    assert_eq!(
        (
            args.auth,
            args.port,
            args.strict_config,
            args.chat_completions_compat,
            args.config_overrides.raw_overrides,
        ),
        (
            ProxyAuth::Chatgpt,
            Some(8080),
            true,
            Some(false),
            vec!["openai_base_url=\"https://example.test/backend-api/codex\"".to_string()],
        )
    );
}

#[test]
fn stdin_auth_remains_the_default() {
    let args =
        Args::try_parse_from(["responses-api-proxy"]).expect("default arguments should parse");

    assert_eq!(
        (args.auth, args.upstream_url, args.chat_completions_compat),
        (ProxyAuth::Stdin, None, None)
    );
}
