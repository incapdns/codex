use axum::Json;
use axum::http::Method;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use serde::Serialize;

const MODELS_PATH: &str = "/v1/models";

// Keep this allowlist aligned with the intentionally exposed ChatGPT/Codex model picker entries.
// The Codex backend catalog does not provide public API creation timestamps, so compatibility
// objects use the Unix epoch as a stable unknown-value sentinel.
const EXPOSED_MODELS: &[Model] = &[
    Model::new("gpt-5.6-sol"),
    Model::new("gpt-5.6-terra"),
    Model::new("gpt-5.6-luna"),
    Model::new("gpt-daybreak-blue-latest"),
    Model::new("gpt-5.5"),
    Model::new("gpt-5.4"),
    Model::new("gpt-5.4-mini"),
];

#[derive(Clone, Copy, Debug, Serialize)]
struct Model {
    id: &'static str,
    object: &'static str,
    created: i64,
    owned_by: &'static str,
}

impl Model {
    const fn new(id: &'static str) -> Self {
        Self {
            id,
            object: "model",
            created: 0,
            owned_by: "openai",
        }
    }
}

#[derive(Serialize)]
struct ModelList {
    object: &'static str,
    data: &'static [Model],
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

pub(crate) fn handle(method: &Method, path: &str) -> Option<Response> {
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

    Some(match model_id {
        None => Json(ModelList {
            object: "list",
            data: EXPOSED_MODELS,
        })
        .into_response(),
        Some(model_id) if !model_id.is_empty() && !model_id.contains('/') => EXPOSED_MODELS
            .iter()
            .find(|model| model.id == model_id)
            .copied()
            .map(Json)
            .map(IntoResponse::into_response)
            .unwrap_or_else(|| model_not_found(model_id)),
        Some(model_id) => model_not_found(model_id),
    })
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
