use std::collections::BTreeMap;
use std::collections::HashSet;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use axum::body::Body;
use axum::body::Bytes;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::Method;
use axum::http::StatusCode;
use axum::http::Uri;
use axum::response::IntoResponse;
use axum::response::Response;
use eventsource_stream::Eventsource;
use futures::Stream;
use futures::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Map;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::chatgpt::ChatgptState;
use crate::dump::ExchangeDump;
use crate::routes::resolve_responses_route;

pub(crate) const DEFAULT_COMPACT_AFTER_ITEMS: usize = 80;
pub(crate) const DEFAULT_STORE_FILENAME: &str = "responses_api_proxy_conversations.json";
const STORE_VERSION: u32 = 1;
const MAX_ITEMS_PER_REQUEST: usize = 20;
const MAX_METADATA_ENTRIES: usize = 16;
const MAX_METADATA_KEY_BYTES: usize = 64;
const MAX_METADATA_VALUE_BYTES: usize = 512;

#[derive(Clone)]
pub(crate) struct ConversationStore {
    data: Arc<Mutex<StoreData>>,
    persistence: mpsc::UnboundedSender<PersistRequest>,
    unsupported_compaction_models: Arc<Mutex<HashSet<String>>>,
    compact_after_items: usize,
}

struct PersistRequest {
    bytes: Vec<u8>,
    completion: oneshot::Sender<Result<(), String>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoreData {
    version: u32,
    #[serde(default)]
    conversations: BTreeMap<String, ConversationRecord>,
}

impl Default for StoreData {
    fn default() -> Self {
        Self {
            version: STORE_VERSION,
            conversations: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ConversationRecord {
    id: String,
    created_at: i64,
    metadata: Value,
    #[serde(default)]
    deleted: bool,
    #[serde(default)]
    items: Vec<StoredItem>,
    #[serde(default)]
    checkpoint: Option<CompactionCheckpoint>,
    #[serde(default, skip)]
    response_in_flight: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredItem {
    value: Value,
    /// True when this proxy generated the item ID rather than receiving it from an upstream.
    generated_id: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CompactionCheckpoint {
    model: String,
    output: Vec<Value>,
    item_count: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedResponse {
    conversation_id: String,
    model: String,
    payload: Map<String, Value>,
    context: Vec<Value>,
    new_items: Vec<StoredItem>,
    should_compact: bool,
    history_item_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ConversationsRoute {
    CreateConversation,
    RetrieveConversation {
        conversation_id: String,
    },
    UpdateConversation {
        conversation_id: String,
    },
    DeleteConversation {
        conversation_id: String,
    },
    CreateItems {
        conversation_id: String,
    },
    ListItems {
        conversation_id: String,
        query: ListQuery,
    },
    RetrieveItem {
        conversation_id: String,
        item_id: String,
    },
    DeleteItem {
        conversation_id: String,
        item_id: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ListQuery {
    after: Option<String>,
    limit: usize,
    order: ListOrder,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ListOrder {
    Asc,
    Desc,
}

#[derive(Debug)]
pub(crate) struct ConversationError {
    status: StatusCode,
    message: String,
    param: Option<String>,
    code: &'static str,
}

impl ConversationError {
    pub(crate) fn invalid(param: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
            param: Some(param.into()),
            code: "invalid_parameter",
        }
    }

    fn not_found(kind: &'static str, id: &str) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: format!("No {kind} found with ID `{id}`"),
            param: None,
            code: if kind == "conversation" {
                "conversation_not_found"
            } else {
                "conversation_item_not_found"
            },
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
            param: Some("conversation".to_string()),
            code: "conversation_response_in_flight",
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
            param: None,
            code: "conversation_store_error",
        }
    }

    pub(crate) fn into_response(self) -> Response {
        (
            self.status,
            axum::Json(ErrorEnvelope {
                error: ErrorBody {
                    message: self.message,
                    r#type: if self.status == StatusCode::INTERNAL_SERVER_ERROR {
                        "proxy_error"
                    } else {
                        "invalid_request_error"
                    },
                    param: self.param,
                    code: self.code,
                },
            }),
        )
            .into_response()
    }
}

impl std::fmt::Display for ConversationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ConversationError {}

#[derive(Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    message: String,
    r#type: &'static str,
    param: Option<String>,
    code: &'static str,
}

impl ConversationStore {
    pub(crate) async fn load(path: PathBuf, compact_after_items: usize) -> Result<Self> {
        let data = match tokio::fs::read(&path).await {
            Ok(bytes) => {
                let data: StoreData = serde_json::from_slice(&bytes)
                    .with_context(|| format!("parsing conversation store {}", path.display()))?;
                if data.version != STORE_VERSION {
                    anyhow::bail!(
                        "unsupported conversation store version {} in {}",
                        data.version,
                        path.display()
                    );
                }
                data
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => StoreData::default(),
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("reading conversation store {}", path.display()));
            }
        };
        let (persistence, requests) = mpsc::unbounded_channel();
        tokio::spawn(persistence_worker(path, requests));
        Ok(Self {
            data: Arc::new(Mutex::new(data)),
            persistence,
            unsupported_compaction_models: Arc::new(Mutex::new(HashSet::new())),
            compact_after_items,
        })
    }

    #[cfg(test)]
    async fn new_for_testing(path: PathBuf, compact_after_items: usize) -> Self {
        Self::load(path, compact_after_items).await.unwrap()
    }

    fn enqueue_save(
        &self,
        data: &StoreData,
    ) -> Result<oneshot::Receiver<Result<(), String>>, ConversationError> {
        let bytes = serde_json::to_vec_pretty(data).map_err(|err| {
            ConversationError::internal(format!("Failed to serialize conversation store: {err}"))
        })?;
        let (completion, receiver) = oneshot::channel();
        self.persistence
            .send(PersistRequest { bytes, completion })
            .map_err(|_| ConversationError::internal("Conversation persistence task stopped"))?;
        Ok(receiver)
    }

    pub(crate) async fn handle(&self, method: &Method, uri: &Uri, body: &[u8]) -> Option<Response> {
        if !uri.path().starts_with("/v1/conversations") {
            return None;
        }
        let route = match resolve_route(method, uri) {
            Ok(route) => route,
            Err(error) => return Some(error.into_response()),
        };
        let result = match route {
            ConversationsRoute::CreateConversation => self.create_conversation(body).await,
            ConversationsRoute::RetrieveConversation { conversation_id } => {
                self.retrieve_conversation(&conversation_id).await
            }
            ConversationsRoute::UpdateConversation { conversation_id } => {
                self.update_conversation(&conversation_id, body).await
            }
            ConversationsRoute::DeleteConversation { conversation_id } => {
                self.delete_conversation(&conversation_id).await
            }
            ConversationsRoute::CreateItems { conversation_id } => {
                self.create_items(&conversation_id, body).await
            }
            ConversationsRoute::ListItems {
                conversation_id,
                query,
            } => self.list_items(&conversation_id, query).await,
            ConversationsRoute::RetrieveItem {
                conversation_id,
                item_id,
            } => self.retrieve_item(&conversation_id, &item_id).await,
            ConversationsRoute::DeleteItem {
                conversation_id,
                item_id,
            } => self.delete_item(&conversation_id, &item_id).await,
        };
        Some(match result {
            Ok(value) => axum::Json(value).into_response(),
            Err(error) => error.into_response(),
        })
    }

    async fn create_conversation(&self, body: &[u8]) -> Result<Value, ConversationError> {
        let request = parse_optional_object(body)?;
        let metadata = validate_metadata(request.get("metadata"))?;
        let items = match request.get("items") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Array(items)) => {
                if items.len() > MAX_ITEMS_PER_REQUEST {
                    return Err(ConversationError::invalid(
                        "items",
                        format!("You may add at most {MAX_ITEMS_PER_REQUEST} items at a time"),
                    ));
                }
                normalize_item_values(items, "items")?
            }
            Some(_) => {
                return Err(ConversationError::invalid(
                    "items",
                    "`items` must be an array or null",
                ));
            }
        };
        reject_unknown_fields(&request, &["items", "metadata"])?;

        let id = new_id("conv");
        let record = ConversationRecord {
            id: id.clone(),
            created_at: unix_timestamp(),
            metadata,
            deleted: false,
            items,
            checkpoint: None,
            response_in_flight: false,
        };
        let response = conversation_json(&record);
        let persistence = {
            let mut data = self.data.lock().await;
            data.conversations.insert(id, record);
            self.enqueue_save(&data)?
        };
        await_persistence(persistence).await?;
        Ok(response)
    }

    async fn retrieve_conversation(&self, id: &str) -> Result<Value, ConversationError> {
        let data = self.data.lock().await;
        let record = live_conversation(&data, id)?;
        Ok(conversation_json(record))
    }

    async fn update_conversation(&self, id: &str, body: &[u8]) -> Result<Value, ConversationError> {
        let request = parse_required_object(body)?;
        reject_unknown_fields(&request, &["metadata"])?;
        if !request.contains_key("metadata") {
            return Err(ConversationError::invalid(
                "metadata",
                "`metadata` is required",
            ));
        }
        let metadata = validate_metadata(request.get("metadata"))?;
        let (response, persistence) = {
            let mut data = self.data.lock().await;
            let record = live_conversation_mut(&mut data, id)?;
            record.metadata = metadata;
            let response = conversation_json(record);
            let persistence = self.enqueue_save(&data)?;
            (response, persistence)
        };
        await_persistence(persistence).await?;
        Ok(response)
    }

    async fn delete_conversation(&self, id: &str) -> Result<Value, ConversationError> {
        let persistence = {
            let mut data = self.data.lock().await;
            let record = live_conversation_mut(&mut data, id)?;
            if record.response_in_flight {
                return Err(ConversationError::conflict(
                    "Cannot delete a conversation while a response is in flight",
                ));
            }
            // The public contract says deleting a conversation does not delete its items.
            record.deleted = true;
            record.checkpoint = None;
            self.enqueue_save(&data)?
        };
        await_persistence(persistence).await?;
        Ok(serde_json::json!({
            "id": id,
            "object": "conversation.deleted",
            "deleted": true,
        }))
    }

    async fn create_items(&self, id: &str, body: &[u8]) -> Result<Value, ConversationError> {
        let request = parse_required_object(body)?;
        reject_unknown_fields(&request, &["items"])?;
        let items = request
            .get("items")
            .ok_or_else(|| ConversationError::invalid("items", "`items` is required"))?;
        let items = items
            .as_array()
            .ok_or_else(|| ConversationError::invalid("items", "`items` must be an array"))?;
        if items.len() > MAX_ITEMS_PER_REQUEST {
            return Err(ConversationError::invalid(
                "items",
                format!("You may add at most {MAX_ITEMS_PER_REQUEST} items at a time"),
            ));
        }
        let items = normalize_item_values(items, "items")?;
        let response_items = items
            .iter()
            .map(|item| item.value.clone())
            .collect::<Vec<_>>();

        let persistence = {
            let mut data = self.data.lock().await;
            let record = live_conversation_mut(&mut data, id)?;
            if record.response_in_flight {
                return Err(ConversationError::conflict(
                    "Cannot add items while a response is in flight",
                ));
            }
            ensure_unique_item_ids(record, &items)?;
            record.items.extend(items);
            self.enqueue_save(&data)?
        };
        await_persistence(persistence).await?;
        Ok(item_list_json(response_items, false))
    }

    async fn list_items(&self, id: &str, query: ListQuery) -> Result<Value, ConversationError> {
        let data = self.data.lock().await;
        let record = live_conversation(&data, id)?;
        let mut indexes = (0..record.items.len()).collect::<Vec<_>>();
        if query.order == ListOrder::Desc {
            indexes.reverse();
        }
        let start = match query.after.as_deref() {
            None => 0,
            Some(after) => indexes
                .iter()
                .position(|index| item_id(&record.items[*index].value) == Some(after))
                .map(|index| index + 1)
                .ok_or_else(|| ConversationError::not_found("conversation item", after))?,
        };
        let has_more = indexes.len().saturating_sub(start) > query.limit;
        let values = indexes
            .into_iter()
            .skip(start)
            .take(query.limit)
            .map(|index| record.items[index].value.clone())
            .collect();
        Ok(item_list_json(values, has_more))
    }

    async fn retrieve_item(&self, id: &str, item_id: &str) -> Result<Value, ConversationError> {
        let data = self.data.lock().await;
        let record = live_conversation(&data, id)?;
        record
            .items
            .iter()
            .find(|item| super::conversations::item_id(&item.value) == Some(item_id))
            .map(|item| item.value.clone())
            .ok_or_else(|| ConversationError::not_found("conversation item", item_id))
    }

    async fn delete_item(&self, id: &str, item_id: &str) -> Result<Value, ConversationError> {
        let (response, persistence) = {
            let mut data = self.data.lock().await;
            let record = live_conversation_mut(&mut data, id)?;
            if record.response_in_flight {
                return Err(ConversationError::conflict(
                    "Cannot delete items while a response is in flight",
                ));
            }
            let index = record
                .items
                .iter()
                .position(|item| super::conversations::item_id(&item.value) == Some(item_id))
                .ok_or_else(|| ConversationError::not_found("conversation item", item_id))?;
            record.items.remove(index);
            record.checkpoint = None;
            let response = conversation_json(record);
            let persistence = self.enqueue_save(&data)?;
            (response, persistence)
        };
        await_persistence(persistence).await?;
        Ok(response)
    }

    pub(crate) async fn begin_response(
        &self,
        body: &[u8],
    ) -> Result<Option<PreparedResponse>, ConversationError> {
        let value: Value = match serde_json::from_slice(body) {
            Ok(value) => value,
            Err(_) => return Ok(None),
        };
        let Some(mut payload) = value.as_object().cloned() else {
            return Ok(None);
        };
        let Some(conversation) = payload.get("conversation").filter(|value| !value.is_null())
        else {
            return Ok(None);
        };
        let conversation_id = match conversation {
            Value::String(id) => id.clone(),
            Value::Object(conversation) => conversation
                .get("id")
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| {
                    ConversationError::invalid(
                        "conversation.id",
                        "Conversation object requires a string `id`",
                    )
                })?,
            _ => {
                return Err(ConversationError::invalid(
                    "conversation",
                    "`conversation` must be an ID string or an object containing `id`",
                ));
            }
        };
        if payload
            .get("previous_response_id")
            .is_some_and(|value| !value.is_null())
        {
            return Err(ConversationError::invalid(
                "previous_response_id",
                "`previous_response_id` cannot be used together with `conversation`",
            ));
        }
        let model = payload
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let new_items = normalize_response_input(payload.get("input"))?;
        payload.remove("conversation");
        payload.remove("previous_response_id");
        payload.insert("store".to_string(), Value::Bool(false));
        ensure_reasoning_encrypted_content(&mut payload)?;

        let (context, should_compact, history_item_count) = {
            let mut data = self.data.lock().await;
            let record = live_conversation_mut(&mut data, &conversation_id)?;
            if record.response_in_flight {
                return Err(ConversationError::conflict(
                    "Only one response at a time may update a local conversation",
                ));
            }
            record.response_in_flight = true;
            let history_item_count = record.items.len();
            let (mut context, tail_count) = match record.checkpoint.as_ref() {
                Some(checkpoint)
                    if checkpoint.model == model && checkpoint.item_count <= record.items.len() =>
                {
                    let mut context = checkpoint.output.clone();
                    context.extend(
                        record.items[checkpoint.item_count..]
                            .iter()
                            .map(item_for_upstream),
                    );
                    (context, record.items.len() - checkpoint.item_count)
                }
                _ => (
                    record.items.iter().map(item_for_upstream).collect(),
                    record.items.len(),
                ),
            };
            // Avoid carrying a checkpoint from another model. It remains in the logical
            // store until a successful compaction replaces it.
            let should_compact = self.compact_after_items > 0
                && !model.is_empty()
                && tail_count >= self.compact_after_items;
            if context.is_empty() {
                context = Vec::new();
            }
            (context, should_compact, history_item_count)
        };

        Ok(Some(PreparedResponse {
            conversation_id,
            model,
            payload,
            context,
            new_items,
            should_compact,
            history_item_count,
        }))
    }

    pub(crate) async fn maybe_compact(
        &self,
        state: &ChatgptState,
        incoming_headers: &HeaderMap,
        prepared: &mut PreparedResponse,
    ) {
        if !prepared.should_compact {
            return;
        }
        if self
            .unsupported_compaction_models
            .lock()
            .await
            .contains(&prepared.model)
        {
            return;
        }
        let Some(route) = resolve_responses_route("POST", "/v1/responses/compact") else {
            return;
        };
        let body = match serde_json::to_vec(&serde_json::json!({
            "model": prepared.model,
            "input": prepared.context,
        })) {
            Ok(body) => Bytes::from(body),
            Err(err) => {
                eprintln!("responses-api-proxy failed to encode compaction request: {err}");
                return;
            }
        };
        let response = match crate::chatgpt::forward_request(
            state,
            &route,
            incoming_headers.clone(),
            body,
        )
        .await
        {
            Ok(response) => response,
            Err(err) => {
                eprintln!(
                    "responses-api-proxy conversation compaction failed; using full context: {err:#}"
                );
                return;
            }
        };
        let status = response.status();
        let body = match response.bytes().await {
            Ok(body) => body,
            Err(err) => {
                eprintln!(
                    "responses-api-proxy failed to read compaction response; using full context: {err}"
                );
                return;
            }
        };
        if !status.is_success() {
            let message = String::from_utf8_lossy(&body);
            if matches!(
                status,
                StatusCode::NOT_FOUND
                    | StatusCode::METHOD_NOT_ALLOWED
                    | StatusCode::NOT_IMPLEMENTED
            ) || (status == StatusCode::BAD_REQUEST
                && message.to_ascii_lowercase().contains("not supported"))
            {
                self.unsupported_compaction_models
                    .lock()
                    .await
                    .insert(prepared.model.clone());
            }
            eprintln!(
                "responses-api-proxy compaction returned {status}; using uncompacted context"
            );
            return;
        }
        let compacted: Value = match serde_json::from_slice(&body) {
            Ok(compacted) => compacted,
            Err(err) => {
                eprintln!(
                    "responses-api-proxy received invalid compaction JSON; using full context: {err}"
                );
                return;
            }
        };
        let Some(output) = compacted.get("output").and_then(Value::as_array).cloned() else {
            eprintln!("responses-api-proxy compaction response omitted output; using full context");
            return;
        };
        if output.is_empty() {
            eprintln!("responses-api-proxy compaction returned no items; using full context");
            return;
        }

        let checkpoint = CompactionCheckpoint {
            model: prepared.model.clone(),
            output: output.clone(),
            item_count: prepared.history_item_count,
        };
        let persistence = {
            let mut data = self.data.lock().await;
            let Some(record) = data.conversations.get_mut(&prepared.conversation_id) else {
                return;
            };
            if record.deleted || record.items.len() != prepared.history_item_count {
                return;
            }
            record.checkpoint = Some(checkpoint);
            match self.enqueue_save(&data) {
                Ok(persistence) => persistence,
                Err(err) => {
                    eprintln!("responses-api-proxy failed to persist compaction checkpoint: {err}");
                    return;
                }
            }
        };
        if let Err(err) = await_persistence(persistence).await {
            eprintln!("responses-api-proxy failed to persist compaction checkpoint: {err}");
            return;
        }
        eprintln!(
            "responses-api-proxy compacted conversation {} after {} items",
            prepared.conversation_id, prepared.history_item_count
        );
        prepared.context = output;
        prepared.should_compact = false;
    }

    pub(crate) fn upstream_body(
        &self,
        prepared: &PreparedResponse,
    ) -> Result<Bytes, ConversationError> {
        let mut payload = prepared.payload.clone();
        let mut input = prepared.context.clone();
        input.extend(prepared.new_items.iter().map(item_for_upstream));
        payload.insert("input".to_string(), Value::Array(input));
        serde_json::to_vec(&Value::Object(payload))
            .map(Bytes::from)
            .map_err(|err| {
                ConversationError::internal(format!(
                    "Failed to encode conversation response request: {err}"
                ))
            })
    }

    pub(crate) async fn abort_response(&self, prepared: &PreparedResponse) {
        let mut data = self.data.lock().await;
        if let Some(record) = data.conversations.get_mut(&prepared.conversation_id) {
            record.response_in_flight = false;
        }
    }

    async fn complete_response(
        &self,
        prepared: &PreparedResponse,
        output: Vec<Value>,
    ) -> Result<(), ConversationError> {
        let persistence = {
            let mut data = self.data.lock().await;
            let record = live_conversation_mut(&mut data, &prepared.conversation_id)?;
            ensure_unique_item_ids(record, &prepared.new_items)?;
            record.items.extend(prepared.new_items.clone());
            let mut existing_ids = record
                .items
                .iter()
                .filter_map(|item| item_id(&item.value).map(str::to_string))
                .collect::<HashSet<_>>();
            for value in output {
                let mut value = value;
                crate::chatgpt::normalize_response_item_collections(&mut value);
                let generated_id = ensure_item_id(&mut value);
                if let Some(id) = item_id(&value) {
                    if existing_ids.contains(id) {
                        continue;
                    }
                    existing_ids.insert(id.to_string());
                }
                record.items.push(StoredItem {
                    value,
                    generated_id,
                });
            }
            record.response_in_flight = false;
            self.enqueue_save(&data)?
        };
        await_persistence(persistence).await
    }

    pub(crate) async fn adapt_upstream_response(
        self: Arc<Self>,
        upstream: reqwest::Response,
        prepared: PreparedResponse,
        exchange_dump: Option<ExchangeDump>,
    ) -> Response {
        if !upstream.status().is_success() {
            self.abort_response(&prepared).await;
            return crate::chatgpt::upstream_response(upstream, exchange_dump);
        }
        let is_sse = upstream
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/event-stream"))
            || prepared
                .payload
                .get("stream")
                .and_then(Value::as_bool)
                .unwrap_or(false);
        if is_sse {
            self.adapt_sse_response(upstream, prepared, exchange_dump)
        } else {
            self.adapt_json_response(upstream, prepared, exchange_dump)
                .await
        }
    }

    fn adapt_sse_response(
        self: Arc<Self>,
        upstream: reqwest::Response,
        prepared: PreparedResponse,
        exchange_dump: Option<ExchangeDump>,
    ) -> Response {
        let status = upstream.status();
        let headers = translated_headers(upstream.headers(), "text/event-stream");
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<Result<Bytes, io::Error>>(32);
        tokio::spawn(async move {
            let mut events = Box::pin(upstream.bytes_stream().eventsource());
            let mut terminal = false;
            let mut completed_output = Vec::new();
            while let Some(event) = events.next().await {
                let event = match event {
                    Ok(event) => event,
                    Err(err) => {
                        let _ = sender.send(Err(io::Error::other(err.to_string()))).await;
                        break;
                    }
                };
                if event.data == "[DONE]" {
                    let _ = sender.send(Ok(encode_sse(&event.event, "[DONE]"))).await;
                    continue;
                }
                let mut value: Value = match serde_json::from_str(&event.data) {
                    Ok(value) => value,
                    Err(_) => {
                        let _ = sender.send(Ok(encode_sse(&event.event, &event.data))).await;
                        continue;
                    }
                };
                attach_conversation(&mut value, &prepared.conversation_id);
                match value.get("type").and_then(Value::as_str) {
                    Some("response.output_item.done") => {
                        if let Some(item) = value.get("item").cloned() {
                            completed_output.push(item);
                        }
                    }
                    Some("response.completed" | "response.incomplete") => {
                        let mut output = std::mem::take(&mut completed_output);
                        output.extend(
                            value
                                .get("response")
                                .and_then(|response| response.get("output"))
                                .and_then(Value::as_array)
                                .cloned()
                                .unwrap_or_default(),
                        );
                        if let Err(err) = self.complete_response(&prepared, output).await {
                            eprintln!(
                                "responses-api-proxy failed to persist conversation response: {err}"
                            );
                        }
                        terminal = true;
                    }
                    Some("response.failed") => {
                        self.abort_response(&prepared).await;
                        terminal = true;
                    }
                    _ => {}
                }
                let _ = sender
                    .send(Ok(encode_sse(&event.event, &value.to_string())))
                    .await;
            }
            if !terminal {
                self.abort_response(&prepared).await;
            }
        });
        let stream: CompatStream = Box::pin(async_stream::stream! {
            while let Some(item) = receiver.recv().await {
                yield item;
            }
        });
        downstream_response(status, headers, stream, exchange_dump)
    }

    async fn adapt_json_response(
        &self,
        upstream: reqwest::Response,
        prepared: PreparedResponse,
        exchange_dump: Option<ExchangeDump>,
    ) -> Response {
        let status = upstream.status();
        let headers = translated_headers(upstream.headers(), "application/json");
        let body = match upstream.bytes().await {
            Ok(body) => body,
            Err(err) => {
                self.abort_response(&prepared).await;
                return ConversationError::internal(format!(
                    "Failed to read Responses body: {err}"
                ))
                .into_response();
            }
        };
        let mut value: Value = match serde_json::from_slice(&body) {
            Ok(value) => value,
            Err(_) => {
                self.abort_response(&prepared).await;
                return buffered_downstream_response(status, headers, body, exchange_dump);
            }
        };
        attach_conversation_object(&mut value, &prepared.conversation_id);
        let terminal = matches!(
            value.get("status").and_then(Value::as_str),
            Some("completed" | "incomplete")
        );
        if terminal {
            let output = value
                .get("output")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if let Err(err) = self.complete_response(&prepared, output).await {
                return err.into_response();
            }
        } else {
            self.abort_response(&prepared).await;
        }
        let body = serde_json::to_vec(&value).map(Bytes::from).unwrap_or(body);
        buffered_downstream_response(status, headers, body, exchange_dump)
    }
}

type CompatStream = Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send>>;

fn encode_sse(event: &str, data: &str) -> Bytes {
    if event.is_empty() {
        Bytes::from(format!("data: {data}\n\n"))
    } else {
        Bytes::from(format!("event: {event}\ndata: {data}\n\n"))
    }
}

fn downstream_response(
    status: StatusCode,
    headers: HeaderMap,
    stream: CompatStream,
    exchange_dump: Option<ExchangeDump>,
) -> Response {
    let body = match exchange_dump {
        Some(exchange_dump) => {
            Body::from_stream(exchange_dump.tee_response_stream(status.as_u16(), &headers, stream))
        }
        None => Body::from_stream(stream),
    };
    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

fn buffered_downstream_response(
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
    exchange_dump: Option<ExchangeDump>,
) -> Response {
    let stream: CompatStream = Box::pin(futures::stream::once(async move {
        Ok::<Bytes, io::Error>(body)
    }));
    downstream_response(status, headers, stream, exchange_dump)
}

fn translated_headers(upstream: &HeaderMap, content_type: &'static str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in upstream {
        if !crate::chatgpt::is_filtered_response_header(name)
            && !matches!(
                name.as_str(),
                "content-encoding" | "content-length" | "content-type"
            )
        {
            headers.append(name, value.clone());
        }
    }
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static(content_type),
    );
    headers
}

fn normalize_response_input(value: Option<&Value>) -> Result<Vec<StoredItem>, ConversationError> {
    match value {
        None => Ok(Vec::new()),
        Some(Value::Null) => Err(ConversationError::invalid(
            "input",
            "`input` must be a string or an array",
        )),
        Some(Value::String(text)) => normalize_item_values(
            &[serde_json::json!({
                "type": "message",
                "role": "user",
                "content": text,
            })],
            "input",
        ),
        Some(Value::Array(items)) => normalize_item_values(items, "input"),
        Some(_) => Err(ConversationError::invalid(
            "input",
            "`input` must be a string or an array",
        )),
    }
}

fn item_for_upstream(item: &StoredItem) -> Value {
    let mut value = item.value.clone();
    crate::chatgpt::normalize_response_item_collections(&mut value);
    if item.generated_id
        && let Some(object) = value.as_object_mut()
    {
        object.remove("id");
        if object.get("type").and_then(Value::as_str) == Some("message") {
            object.remove("status");
        }
    }
    value
}

fn ensure_reasoning_encrypted_content(
    payload: &mut Map<String, Value>,
) -> Result<(), ConversationError> {
    match payload.entry("include") {
        serde_json::map::Entry::Vacant(entry) => {
            entry.insert(serde_json::json!(["reasoning.encrypted_content"]));
        }
        serde_json::map::Entry::Occupied(mut entry) => match entry.get_mut() {
            Value::Null => {
                entry.insert(serde_json::json!(["reasoning.encrypted_content"]));
            }
            Value::Array(include) => {
                if !include
                    .iter()
                    .any(|value| value.as_str() == Some("reasoning.encrypted_content"))
                {
                    include.push(Value::String("reasoning.encrypted_content".to_string()));
                }
            }
            include => {
                let value = include.take();
                let mut values = vec![value];
                if !values
                    .iter()
                    .any(|value| value.as_str() == Some("reasoning.encrypted_content"))
                {
                    values.push(Value::String("reasoning.encrypted_content".to_string()));
                }
                *include = Value::Array(values);
            }
        },
    }
    Ok(())
}

fn attach_conversation(event: &mut Value, conversation_id: &str) {
    if let Some(response) = event.get_mut("response") {
        attach_conversation_object(response, conversation_id);
    }
}

fn attach_conversation_object(response: &mut Value, conversation_id: &str) {
    if let Some(response) = response.as_object_mut() {
        response.insert(
            "conversation".to_string(),
            serde_json::json!({"id": conversation_id}),
        );
    }
}

fn resolve_route(method: &Method, uri: &Uri) -> Result<ConversationsRoute, ConversationError> {
    let segments = uri
        .path()
        .strip_prefix("/v1/conversations")
        .ok_or_else(|| ConversationError::invalid("path", "Invalid Conversations path"))?;
    if !segments.is_empty() && !segments.starts_with('/') {
        return Err(ConversationError::invalid(
            "path",
            "Invalid Conversations path",
        ));
    }
    let segments = if segments.is_empty() {
        Vec::new()
    } else {
        let segments = segments
            .strip_prefix('/')
            .ok_or_else(|| ConversationError::invalid("path", "Invalid Conversations path"))?;
        if segments.is_empty() {
            return Err(ConversationError::invalid(
                "path",
                "Invalid Conversations path",
            ));
        }
        segments.split('/').collect::<Vec<_>>()
    };
    if segments.iter().any(|segment| !valid_segment(segment)) {
        return Err(ConversationError::invalid(
            "path",
            "Invalid Conversations resource ID",
        ));
    }

    match (method, &segments[..]) {
        (&Method::POST, []) if uri.query().is_none() => Ok(ConversationsRoute::CreateConversation),
        (&Method::GET, [conversation_id]) if uri.query().is_none() => {
            Ok(ConversationsRoute::RetrieveConversation {
                conversation_id: (*conversation_id).to_string(),
            })
        }
        (&Method::POST, [conversation_id]) if uri.query().is_none() => {
            Ok(ConversationsRoute::UpdateConversation {
                conversation_id: (*conversation_id).to_string(),
            })
        }
        (&Method::DELETE, [conversation_id]) if uri.query().is_none() => {
            Ok(ConversationsRoute::DeleteConversation {
                conversation_id: (*conversation_id).to_string(),
            })
        }
        (&Method::POST, [conversation_id, "items"]) => {
            validate_include_only(uri.query())?;
            Ok(ConversationsRoute::CreateItems {
                conversation_id: (*conversation_id).to_string(),
            })
        }
        (&Method::GET, [conversation_id, "items"]) => Ok(ConversationsRoute::ListItems {
            conversation_id: (*conversation_id).to_string(),
            query: parse_list_query(uri.query())?,
        }),
        (&Method::GET, [conversation_id, "items", item_id]) => {
            validate_include_only(uri.query())?;
            Ok(ConversationsRoute::RetrieveItem {
                conversation_id: (*conversation_id).to_string(),
                item_id: (*item_id).to_string(),
            })
        }
        (&Method::DELETE, [conversation_id, "items", item_id]) if uri.query().is_none() => {
            Ok(ConversationsRoute::DeleteItem {
                conversation_id: (*conversation_id).to_string(),
                item_id: (*item_id).to_string(),
            })
        }
        _ => Err(ConversationError {
            status: StatusCode::METHOD_NOT_ALLOWED,
            message: "Unsupported Conversations method or path".to_string(),
            param: None,
            code: "unsupported_route",
        }),
    }
}

fn parse_list_query(query: Option<&str>) -> Result<ListQuery, ConversationError> {
    let mut parsed = ListQuery {
        after: None,
        limit: 20,
        order: ListOrder::Desc,
    };
    let Some(query) = query else {
        return Ok(parsed);
    };
    if query.is_empty() {
        return Err(ConversationError::invalid("query", "Invalid query string"));
    }
    let url = reqwest::Url::parse(&format!("http://localhost/?{query}"))
        .map_err(|_| ConversationError::invalid("query", "Invalid query string"))?;
    let mut seen = HashSet::new();
    for (name, value) in url.query_pairs() {
        match name.as_ref() {
            "after" if seen.insert("after") && !value.is_empty() => {
                parsed.after = Some(value.into_owned())
            }
            "include" | "include[]" if valid_include(&value) => {}
            "limit" => {
                if !seen.insert("limit") {
                    return Err(ConversationError::invalid(
                        "limit",
                        "`limit` may only be specified once",
                    ));
                }
                let limit = value.parse::<usize>().map_err(|_| {
                    ConversationError::invalid("limit", "`limit` must be an integer")
                })?;
                if !(1..=100).contains(&limit) {
                    return Err(ConversationError::invalid(
                        "limit",
                        "`limit` must be between 1 and 100",
                    ));
                }
                parsed.limit = limit;
            }
            "order" if !seen.insert("order") => {
                return Err(ConversationError::invalid(
                    "order",
                    "`order` may only be specified once",
                ));
            }
            "order" if value == "asc" => parsed.order = ListOrder::Asc,
            "order" if value == "desc" => parsed.order = ListOrder::Desc,
            "order" => {
                return Err(ConversationError::invalid(
                    "order",
                    "`order` must be `asc` or `desc`",
                ));
            }
            _ => {
                return Err(ConversationError::invalid(
                    name.into_owned(),
                    "Unsupported query parameter",
                ));
            }
        }
    }
    Ok(parsed)
}

fn validate_include_only(query: Option<&str>) -> Result<(), ConversationError> {
    let Some(query) = query else {
        return Ok(());
    };
    if query.is_empty() {
        return Err(ConversationError::invalid("query", "Invalid query string"));
    }
    let url = reqwest::Url::parse(&format!("http://localhost/?{query}"))
        .map_err(|_| ConversationError::invalid("query", "Invalid query string"))?;
    for (name, value) in url.query_pairs() {
        if !matches!(name.as_ref(), "include" | "include[]") || !valid_include(&value) {
            return Err(ConversationError::invalid(
                name.into_owned(),
                "Unsupported query parameter",
            ));
        }
    }
    Ok(())
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

fn valid_segment(segment: &str) -> bool {
    !matches!(segment, "" | "." | "..")
        && segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'~'))
}

fn parse_optional_object(body: &[u8]) -> Result<Map<String, Value>, ConversationError> {
    if body.is_empty() {
        return Ok(Map::new());
    }
    parse_required_object(body)
}

fn parse_required_object(body: &[u8]) -> Result<Map<String, Value>, ConversationError> {
    let value: Value = serde_json::from_slice(body)
        .map_err(|err| ConversationError::invalid("body", format!("Invalid JSON body: {err}")))?;
    value
        .as_object()
        .cloned()
        .ok_or_else(|| ConversationError::invalid("body", "Request body must be a JSON object"))
}

fn reject_unknown_fields(
    object: &Map<String, Value>,
    allowed: &[&str],
) -> Result<(), ConversationError> {
    if let Some(field) = object
        .keys()
        .find(|field| !allowed.contains(&field.as_str()))
    {
        return Err(ConversationError::invalid(
            field,
            format!("Unknown parameter `{field}`"),
        ));
    }
    Ok(())
}

fn validate_metadata(value: Option<&Value>) -> Result<Value, ConversationError> {
    let metadata = match value {
        None | Some(Value::Null) => Map::new(),
        Some(Value::Object(metadata)) => metadata.clone(),
        Some(_) => {
            return Err(ConversationError::invalid(
                "metadata",
                "`metadata` must be an object or null",
            ));
        }
    };
    if metadata.len() > MAX_METADATA_ENTRIES {
        return Err(ConversationError::invalid(
            "metadata",
            format!("`metadata` may contain at most {MAX_METADATA_ENTRIES} entries"),
        ));
    }
    for (key, value) in &metadata {
        if key.len() > MAX_METADATA_KEY_BYTES {
            return Err(ConversationError::invalid(
                "metadata",
                format!("Metadata key `{key}` exceeds {MAX_METADATA_KEY_BYTES} bytes"),
            ));
        }
        let Some(value) = value.as_str() else {
            return Err(ConversationError::invalid(
                "metadata",
                "Metadata values must be strings",
            ));
        };
        if value.len() > MAX_METADATA_VALUE_BYTES {
            return Err(ConversationError::invalid(
                "metadata",
                format!("Metadata value for `{key}` exceeds {MAX_METADATA_VALUE_BYTES} bytes"),
            ));
        }
    }
    Ok(Value::Object(metadata))
}

fn normalize_item_values(
    items: &[Value],
    param: &str,
) -> Result<Vec<StoredItem>, ConversationError> {
    let mut ids = HashSet::new();
    items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            crate::chatgpt::validate_response_input_item_collections(
                item,
                &format!("{param}[{index}]"),
            )?;
            let mut item = item.as_object().cloned().ok_or_else(|| {
                ConversationError::invalid(
                    format!("{param}[{index}]"),
                    "Conversation items must be objects",
                )
            })?;
            if !item.contains_key("type") && item.contains_key("role") {
                item.insert("type".to_string(), Value::String("message".to_string()));
            }
            let mut value = Value::Object(item);
            crate::chatgpt::normalize_public_response_input_item(&mut value);
            let Value::Object(mut item) = value else {
                return Err(ConversationError::internal(
                    "Normalized conversation item is not an object",
                ));
            };
            if item.get("type").and_then(Value::as_str) == Some("message") {
                item.entry("status")
                    .or_insert_with(|| Value::String("completed".to_string()));
            }
            let generated_id = !item.contains_key("id");
            if generated_id {
                let prefix = item_id_prefix(item.get("type").and_then(Value::as_str));
                item.insert("id".to_string(), Value::String(new_id(prefix)));
            }
            let id = item.get("id").and_then(Value::as_str).ok_or_else(|| {
                ConversationError::invalid(
                    format!("{param}[{index}].id"),
                    "Conversation item ID must be a string",
                )
            })?;
            if !ids.insert(id.to_string()) {
                return Err(ConversationError::invalid(
                    format!("{param}[{index}].id"),
                    format!("Duplicate conversation item ID `{id}`"),
                ));
            }
            Ok(StoredItem {
                value: Value::Object(item),
                generated_id,
            })
        })
        .collect()
}

fn ensure_unique_item_ids(
    record: &ConversationRecord,
    incoming: &[StoredItem],
) -> Result<(), ConversationError> {
    let existing = record
        .items
        .iter()
        .filter_map(|item| item_id(&item.value))
        .collect::<HashSet<_>>();
    if let Some(id) = incoming
        .iter()
        .filter_map(|item| item_id(&item.value))
        .find(|id| existing.contains(id))
    {
        return Err(ConversationError::invalid(
            "items",
            format!("Conversation item ID `{id}` already exists"),
        ));
    }
    Ok(())
}

fn live_conversation<'a>(
    data: &'a StoreData,
    id: &str,
) -> Result<&'a ConversationRecord, ConversationError> {
    data.conversations
        .get(id)
        .filter(|record| !record.deleted)
        .ok_or_else(|| ConversationError::not_found("conversation", id))
}

fn live_conversation_mut<'a>(
    data: &'a mut StoreData,
    id: &str,
) -> Result<&'a mut ConversationRecord, ConversationError> {
    data.conversations
        .get_mut(id)
        .filter(|record| !record.deleted)
        .ok_or_else(|| ConversationError::not_found("conversation", id))
}

fn conversation_json(record: &ConversationRecord) -> Value {
    serde_json::json!({
        "id": record.id,
        "object": "conversation",
        "created_at": record.created_at,
        "metadata": record.metadata,
    })
}

fn item_list_json(data: Vec<Value>, has_more: bool) -> Value {
    let first_id = data.first().and_then(item_id);
    let last_id = data.last().and_then(item_id);
    serde_json::json!({
        "object": "list",
        "data": data,
        "first_id": first_id,
        "last_id": last_id,
        "has_more": has_more,
    })
}

fn item_id(item: &Value) -> Option<&str> {
    item.get("id").and_then(Value::as_str)
}

fn ensure_item_id(item: &mut Value) -> bool {
    if item_id(item).is_some() {
        return false;
    }
    let item_type = item.get("type").and_then(Value::as_str);
    let id = new_id(item_id_prefix(item_type));
    if let Some(item) = item.as_object_mut() {
        item.insert("id".to_string(), Value::String(id));
        true
    } else {
        false
    }
}

fn item_id_prefix(item_type: Option<&str>) -> &'static str {
    match item_type {
        Some("message") => "msg",
        Some("function_call") => "fc",
        Some("function_call_output") => "fco",
        Some("reasoning") => "rs",
        Some("computer_call") => "cc",
        _ => "item",
    }
}

fn new_id(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

fn unix_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs() as i64)
}

async fn await_persistence(
    completion: oneshot::Receiver<Result<(), String>>,
) -> Result<(), ConversationError> {
    completion
        .await
        .map_err(|_| ConversationError::internal("Conversation persistence task stopped"))?
        .map_err(ConversationError::internal)
}

async fn persistence_worker(path: PathBuf, mut requests: mpsc::UnboundedReceiver<PersistRequest>) {
    while let Some(request) = requests.recv().await {
        let result = persist_store(&path, &request.bytes).await;
        let _ = request.completion.send(result);
    }
}

async fn persist_store(path: &Path, bytes: &[u8]) -> Result<(), String> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|err| format!("Failed to create conversation store directory: {err}"))?;
    }
    let temporary = temporary_path(path);
    tokio::fs::write(&temporary, bytes)
        .await
        .map_err(|err| format!("Failed to write conversation store: {err}"))?;
    #[cfg(unix)]
    tokio::fs::set_permissions(
        &temporary,
        <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o600),
    )
    .await
    .map_err(|err| format!("Failed to secure conversation store permissions: {err}"))?;
    tokio::fs::rename(&temporary, path)
        .await
        .map_err(|err| format!("Failed to replace conversation store: {err}"))
}

fn temporary_path(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("conversations.json");
    path.with_file_name(format!(".{filename}.{}.tmp", uuid::Uuid::new_v4().simple()))
}

#[cfg(test)]
#[path = "conversations_tests.rs"]
mod tests;
