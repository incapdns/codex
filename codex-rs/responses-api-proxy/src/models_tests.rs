use axum::body::to_bytes;
use http::Method;
use http::StatusCode;
use pretty_assertions::assert_eq;

use super::handle;

async fn json_body(response: axum::response::Response) -> serde_json::Value {
    serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body"),
    )
    .expect("JSON response")
}

#[tokio::test]
async fn lists_only_the_explicitly_exposed_models() {
    let response = handle(&Method::GET, "/v1/models").expect("Models route");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        json_body(response).await,
        serde_json::json!({
            "object": "list",
            "data": [
                {"id":"gpt-5.6-sol","object":"model","created":0,"owned_by":"openai"},
                {"id":"gpt-5.6-terra","object":"model","created":0,"owned_by":"openai"},
                {"id":"gpt-5.6-luna","object":"model","created":0,"owned_by":"openai"},
                {"id":"gpt-daybreak-blue-latest","object":"model","created":0,"owned_by":"openai"},
                {"id":"gpt-5.5","object":"model","created":0,"owned_by":"openai"},
                {"id":"gpt-5.4","object":"model","created":0,"owned_by":"openai"},
                {"id":"gpt-5.4-mini","object":"model","created":0,"owned_by":"openai"},
            ],
        })
    );
}

#[tokio::test]
async fn retrieves_each_exposed_model() {
    for model_id in [
        "gpt-5.6-sol",
        "gpt-5.6-terra",
        "gpt-5.6-luna",
        "gpt-daybreak-blue-latest",
        "gpt-5.5",
        "gpt-5.4",
        "gpt-5.4-mini",
    ] {
        let response =
            handle(&Method::GET, &format!("/v1/models/{model_id}")).expect("model retrieve route");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(json_body(response).await["id"], model_id);
    }
}

#[tokio::test]
async fn rejects_unlisted_models_with_the_public_error_shape() {
    let response = handle(&Method::GET, "/v1/models/gpt-5.2").expect("model retrieve route");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        json_body(response).await,
        serde_json::json!({
            "error": {
                "message": "The model `gpt-5.2` does not exist or you do not have access to it.",
                "type": "invalid_request_error",
                "param": "model",
                "code": "model_not_found",
            }
        })
    );
}

#[test]
fn recognizes_only_models_paths_and_get_operations() {
    assert!(handle(&Method::GET, "/v1/models-other").is_none());
    assert_eq!(
        handle(&Method::DELETE, "/v1/models/gpt-5.5")
            .expect("Models route")
            .status(),
        StatusCode::METHOD_NOT_ALLOWED
    );
}
