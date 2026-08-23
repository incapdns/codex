use pretty_assertions::assert_eq;
use reqwest::Url;

use super::resolve_responses_route;

#[test]
fn accepts_the_complete_responses_resource_contract() {
    let cases = [
        ("POST", "/v1/responses", "/backend-api/codex/responses"),
        (
            "GET",
            "/v1/responses/resp_123?include=reasoning.encrypted_content&include_obfuscation=false&starting_after=7",
            "/backend-api/codex/responses/resp_123?tenant=codex&include=reasoning.encrypted_content&include_obfuscation=false&starting_after=7",
        ),
        (
            "DELETE",
            "/v1/responses/resp_123",
            "/backend-api/codex/responses/resp_123?tenant=codex",
        ),
        (
            "POST",
            "/v1/responses/resp_123/cancel",
            "/backend-api/codex/responses/resp_123/cancel?tenant=codex",
        ),
        (
            "POST",
            "/v1/responses/compact",
            "/backend-api/codex/responses/compact?tenant=codex",
        ),
        (
            "POST",
            "/v1/responses/input_tokens",
            "/backend-api/codex/responses/input_tokens?tenant=codex",
        ),
        (
            "GET",
            "/v1/responses/resp_123/input_items?after=item_1&include[]=reasoning.encrypted_content&limit=100&order=asc",
            "/backend-api/codex/responses/resp_123/input_items?tenant=codex&after=item_1&include[]=reasoning.encrypted_content&limit=100&order=asc",
        ),
    ];

    for (method, uri, expected) in cases {
        let route = resolve_responses_route(method, uri).expect(uri);
        let base = if uri == "/v1/responses" {
            Url::parse("https://chatgpt.com/backend-api/codex/responses").unwrap()
        } else {
            Url::parse("https://chatgpt.com/backend-api/codex/responses?tenant=codex").unwrap()
        };
        let upstream = route.upstream_url(&base).expect("upstream URL");
        let actual = match upstream.query() {
            Some(query) => format!("{}?{query}", upstream.path()),
            None => upstream.path().to_string(),
        };
        assert_eq!(actual, expected);
    }
}

#[test]
fn rejects_unsupported_methods_paths_and_queries() {
    let cases = [
        ("GET", "/v1/responses"),
        ("PUT", "/v1/responses/resp_123"),
        ("DELETE", "/v1/responses/resp_123?force=true"),
        ("POST", "/v1/responses?stream=true"),
        ("GET", "/v1/responses/resp_123?unknown=true"),
        ("GET", "/v1/responses/resp_123/input_items?before=item_1"),
        ("GET", "/v1/responses/resp_123/unknown"),
        ("GET", "/v1/responses/../input_items"),
        ("POST", "/v1/responses/resp_123//cancel"),
        ("POST", "/v1/responses/compact/"),
        ("POST", "/v1/responses-other"),
    ];

    for (method, uri) in cases {
        assert_eq!(resolve_responses_route(method, uri), None, "{method} {uri}");
    }
}

#[test]
fn requires_a_create_url_ending_in_responses() {
    let route = resolve_responses_route("POST", "/v1/responses").unwrap();
    let invalid = Url::parse("https://example.test/v1").unwrap();

    assert_eq!(
        route.upstream_url(&invalid).unwrap_err().to_string(),
        "upstream URL path must end in /responses"
    );
}
