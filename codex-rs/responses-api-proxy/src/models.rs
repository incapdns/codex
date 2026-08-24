use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use axum::Json;
use axum::body::Bytes;
use axum::http::HeaderMap;
use axum::http::Method;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use serde::Deserialize;
use serde::Serialize;

use crate::chatgpt::ChatgptState;
use crate::chatgpt::authenticated_request;

const MODELS_PATH: &str = "/v1/models";

#[derive(Clone, Debug, Serialize)]
struct Model {
    id: String,
    object: &'static str,
    created: i64,
    owned_by: &'static str,
}

impl Model {
    fn new(id: String) -> Self {
        Self {
            id,
            object: "model",
            // The Codex catalog does not expose public API creation timestamps.
            created: 0,
            owned_by: "openai",
        }
    }
}

#[derive(Serialize)]
struct ModelList {
    object: &'static str,
    data: Vec<Model>,
}

#[derive(Deserialize)]
struct BackendModelsResponse {
    models: Vec<BackendModel>,
}

/// Minimal projection of the Codex catalog. Unknown fields—including model instructions—are
/// deliberately ignored because they are neither part of the public Models resource nor required
/// to determine API visibility.
#[derive(Deserialize)]
struct BackendModel {
    slug: String,
    #[serde(default)]
    visibility: String,
    #[serde(default)]
    supported_in_api: bool,
}

#[derive(Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    message: String,
    r#type: &'static str,
    param: Option<&'static str>,
    code: &'static str,
}

pub(crate) async fn handle(
    state: &ChatgptState,
    method: &Method,
    path: &str,
    incoming_headers: &HeaderMap,
) -> Option<Response> {
    let model_id = match path {
        MODELS_PATH => None,
        _ => path.strip_prefix("/v1/models/"),
    };
    if path != MODELS_PATH && model_id.is_none() {
        return None;
    }
    if method != Method::GET {
        return Some(error_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "Only GET is supported for the local Models resource".to_string(),
            None,
            "method_not_allowed",
        ));
    }

    let models = match fetch_models(state, incoming_headers.clone()).await {
        Ok(models) => models,
        Err(err) => {
            eprintln!("fetching models failed: {err:#}");
            return Some(error_response(
                StatusCode::BAD_GATEWAY,
                err.to_string(),
                None,
                "upstream_request_failed",
            ));
        }
    };

    Some(match model_id {
        None => Json(ModelList {
            object: "list",
            data: models,
        })
        .into_response(),
        Some(model_id) if !model_id.is_empty() && !model_id.contains('/') => models
            .into_iter()
            .find(|model| model.id == model_id)
            .map(Json)
            .map(IntoResponse::into_response)
            .unwrap_or_else(|| model_not_found(model_id)),
        Some(model_id) => model_not_found(model_id),
    })
}

async fn fetch_models(state: &ChatgptState, incoming_headers: HeaderMap) -> Result<Vec<Model>> {
    let upstream_url = models_upstream_url(&state.upstream_url)?;
    let response = authenticated_request(
        state,
        Method::GET,
        upstream_url,
        incoming_headers,
        Bytes::new(),
    )
    .await
    .context("fetching the Codex model catalog")?;
    let status = response.status();
    let body = response
        .bytes()
        .await
        .context("reading the Codex model catalog")?;
    if !status.is_success() {
        return Err(anyhow!(
            "Codex models endpoint returned {status}: {}",
            String::from_utf8_lossy(&body)
        ));
    }
    let catalog: BackendModelsResponse =
        serde_json::from_slice(&body).context("decoding the Codex model catalog")?;
    Ok(catalog
        .models
        .into_iter()
        .filter(|model| model.visibility == "list" && model.supported_in_api)
        .map(|model| Model::new(model.slug))
        .collect())
}

fn models_upstream_url(responses_url: &reqwest::Url) -> Result<reqwest::Url> {
    let path = responses_url.path();
    let base_path = path
        .strip_suffix("/responses")
        .ok_or_else(|| anyhow!("upstream URL path must end in /responses"))?;
    let mut url = responses_url.clone();
    url.set_path(&format!("{base_path}/models"));

    let existing_query = url
        .query_pairs()
        .filter(|(name, _)| name != "client_version")
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    url.set_query(None);
    {
        let mut query = url.query_pairs_mut();
        query.extend_pairs(existing_query);
        query.append_pair("client_version", &client_version());
    }
    Ok(url)
}

fn client_version() -> String {
    format!(
        "{}.{}.{}",
        env!("CARGO_PKG_VERSION_MAJOR"),
        env!("CARGO_PKG_VERSION_MINOR"),
        env!("CARGO_PKG_VERSION_PATCH")
    )
}

fn model_not_found(model_id: &str) -> Response {
    error_response(
        StatusCode::NOT_FOUND,
        format!("The model `{model_id}` does not exist or you do not have access to it."),
        Some("model"),
        "model_not_found",
    )
}

fn error_response(
    status: StatusCode,
    message: String,
    param: Option<&'static str>,
    code: &'static str,
) -> Response {
    (
        status,
        Json(ErrorEnvelope {
            error: ErrorBody {
                message,
                r#type: "invalid_request_error",
                param,
                code,
            },
        }),
    )
        .into_response()
}

#[cfg(test)]
#[path = "models_tests.rs"]
mod tests;
