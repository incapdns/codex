use anyhow::Result;
use anyhow::bail;
use http::Method;
use reqwest::Url;
use std::collections::HashSet;

const RESPONSES_PATH: &str = "/v1/responses";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResponsesRoute {
    pub(crate) method: Method,
    suffix: Vec<String>,
    query: Option<String>,
}

impl ResponsesRoute {
    pub(crate) fn is_create(&self) -> bool {
        self.method == Method::POST && self.suffix.is_empty()
    }

    pub(crate) fn accepts_response_input(&self) -> bool {
        self.method == Method::POST
            && (self.suffix.is_empty()
                || matches!(self.suffix.as_slice(), [suffix] if suffix == "compact" || suffix == "input_tokens"))
    }

    pub(crate) fn upstream_url(&self, create_url: &Url) -> Result<Url> {
        validate_upstream_create_url(create_url)?;

        let mut url = create_url.clone();
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|()| anyhow::anyhow!("upstream URL cannot be a base URL"))?;
            segments.pop_if_empty();
            for segment in &self.suffix {
                segments.push(segment);
            }
        }

        if let Some(query) = self.query.as_deref() {
            let merged = match url.query() {
                Some(base_query) if !base_query.is_empty() => format!("{base_query}&{query}"),
                _ => query.to_string(),
            };
            url.set_query(Some(&merged));
        }
        Ok(url)
    }
}

pub(crate) fn validate_upstream_create_url(create_url: &Url) -> Result<()> {
    if create_url.path_segments().and_then(Iterator::last) != Some("responses") {
        bail!("upstream URL path must end in /responses");
    }
    Ok(())
}

pub(crate) fn resolve_responses_route(method: &str, uri: &str) -> Option<ResponsesRoute> {
    let (path, query) = match uri.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (uri, None),
    };
    if path.contains('#') || query.is_some_and(str::is_empty) {
        return None;
    }

    let suffix = path.strip_prefix(RESPONSES_PATH)?;
    if !suffix.is_empty() && !suffix.starts_with('/') {
        return None;
    }
    let suffix_without_slash = suffix.strip_prefix('/').unwrap_or_default();
    let segments = if suffix_without_slash.is_empty() {
        Vec::new()
    } else {
        suffix_without_slash.split('/').collect::<Vec<_>>()
    };
    if segments.iter().any(|segment| !valid_segment(segment)) {
        return None;
    }

    let (expected_method, suffix, allowed_query): (&str, Vec<String>, &[&str]) = match &segments[..]
    {
        [] => ("POST", vec![], &[]),
        ["compact"] => ("POST", vec!["compact".to_string()], &[]),
        ["input_tokens"] => ("POST", vec!["input_tokens".to_string()], &[]),
        [response_id] => (
            if method == "DELETE" { "DELETE" } else { "GET" },
            vec![(*response_id).to_string()],
            if method == "GET" {
                &[
                    "include",
                    "include[]",
                    "include_obfuscation",
                    "starting_after",
                ]
            } else {
                &[]
            },
        ),
        [response_id, "cancel"] => (
            "POST",
            vec![(*response_id).to_string(), "cancel".to_string()],
            &[],
        ),
        [response_id, "input_items"] => (
            "GET",
            vec![(*response_id).to_string(), "input_items".to_string()],
            &["after", "include", "include[]", "limit", "order"],
        ),
        _ => return None,
    };

    if method != expected_method || !valid_query(query, allowed_query) {
        return None;
    }
    Some(ResponsesRoute {
        method: Method::from_bytes(method.as_bytes()).ok()?,
        suffix,
        query: query.map(str::to_string),
    })
}

fn valid_segment(segment: &str) -> bool {
    !matches!(segment, "" | "." | "..")
        && segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'~'))
}

fn valid_query(query: Option<&str>, allowed: &[&str]) -> bool {
    let Some(query) = query else {
        return true;
    };
    if allowed.is_empty() || query.contains('#') {
        return false;
    }

    let Ok(url) = Url::parse(&format!("http://localhost/?{query}")) else {
        return false;
    };
    let mut seen = HashSet::new();
    url.query_pairs().all(|(name, value)| {
        if !allowed.contains(&name.as_ref()) {
            return false;
        }
        match name.as_ref() {
            "include" | "include[]" => valid_include(&value),
            "include_obfuscation" => {
                seen.insert("include_obfuscation") && matches!(value.as_ref(), "true" | "false")
            }
            "starting_after" => seen.insert("starting_after") && value.parse::<u64>().is_ok(),
            "after" => seen.insert("after") && !value.is_empty(),
            "limit" => {
                seen.insert("limit")
                    && value
                        .parse::<usize>()
                        .is_ok_and(|limit| (1..=100).contains(&limit))
            }
            "order" => seen.insert("order") && matches!(value.as_ref(), "asc" | "desc"),
            _ => false,
        }
    })
}

fn valid_include(value: &str) -> bool {
    matches!(
        value,
        "web_search_call.action.sources"
            | "web_search_call.results"
            | "code_interpreter_call.outputs"
            | "computer_call_output.output.image_url"
            | "file_search_call.results"
            | "message.input_image.image_url"
            | "message.output_text.logprobs"
            | "reasoning.encrypted_content"
    )
}

#[cfg(test)]
#[path = "routes_tests.rs"]
mod tests;
