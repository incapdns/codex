# codex-responses-api-proxy

#### tl;dr:

Use the existing Codex ChatGPT login and expose it as a local Responses API:

```shell
codex responses-api-proxy --auth chatgpt --port 60001

curl -fsS http://127.0.0.1:60001/v1/models | jq

curl http://127.0.0.1:60001/v1/responses \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpt-5.1-codex","input":"Hello","stream":true}'

curl http://127.0.0.1:60001/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpt-5.1-codex","messages":[{"role":"user","content":"Hello"}]}'
```

The ChatGPT mode loads the normal Codex configuration and credentials. Run `codex login` first;
clients of the local endpoint do not supply an `Authorization` header.

To proxy an OpenAI API key instead:

```
# Launch the proxy, dump request/response pairs to /tmp/proxy
cd path/to/codex/codex-rs
cargo build
echo $OPENAI_API_KEY | ./target/debug/codex-responses-api-proxy \
    --port 60001 \
    --dump-dir /tmp/proxy


# Add this to ~/.codex/config.toml:

[model_providers.codex-responses-api-proxy]
name = 'codex-responses-api-proxy'
base_url = 'http://127.0.0.1:60001/v1'
wire_api='responses'

[profiles.proxy]
model_provider = "codex-responses-api-proxy"


# Use it
codex -p proxy
```

# Detailed docs

A strict HTTP proxy for the OpenAI Responses resource. It supports two credential sources:

- `--auth stdin` (the default) reads an OpenAI API key from stdin and forwards to
  `https://api.openai.com/v1/responses`.
- `--auth chatgpt` loads the managed ChatGPT login and Codex configuration, then forwards to the
  configured model provider. For the built-in OpenAI provider this resolves to
  `https://chatgpt.com/backend-api/codex/responses`.

## Models resource

ChatGPT auth mode exposes the OpenAI-compatible Models read operations locally:

| Operation | Local endpoint |
| --- | --- |
| List | `GET /v1/models` |
| Retrieve | `GET /v1/models/{model}` |

The resource is intentionally restricted to these model IDs:

- `gpt-5.6-sol`
- `gpt-5.6-terra`
- `gpt-5.6-luna`
- `gpt-daybreak-blue-latest`
- `gpt-5.5`
- `gpt-5.4`
- `gpt-5.4-mini`

Unlisted model IDs return `404 model_not_found`. The Codex model catalog does not expose public
creation timestamps, so the required `created` field uses Unix epoch `0` as an explicit unknown
value. Built-in models cannot be deleted through this compatibility resource.

## Responses resource

The proxy exposes the complete REST resource documented in the
[OpenAI Responses API reference](https://developers.openai.com/api/reference/resources/responses):

| Operation | Local endpoint |
| --- | --- |
| Create | `POST /v1/responses` |
| Retrieve | `GET /v1/responses/{response_id}` |
| Delete | `DELETE /v1/responses/{response_id}` |
| Cancel | `POST /v1/responses/{response_id}/cancel` |
| Compact | `POST /v1/responses/compact` |
| Count input tokens | `POST /v1/responses/input_tokens` |
| List input items | `GET /v1/responses/{response_id}/input_items` |

Documented query parameters are forwarded for retrieve (`include`, `include_obfuscation`, and
`starting_after`) and list input items (`after`, `include`, `limit`, and `order`). Request and
response bodies remain opaque, so streaming and future body fields do not require proxy changes.
Unsupported methods, paths, and query parameters are rejected with `403 Forbidden`.

The ChatGPT backend requires `input` to be an item list. In ChatGPT mode, the proxy accepts the
public API's string and easy-message shorthand forms and expands them to equivalent `message` /
`input_text` items before forwarding. It also supplies the backend-required `store: false` and an
empty input list when those optional public-API fields are omitted. Explicit values and already
structured item lists are preserved.

## Conversations compatibility

The ChatGPT Codex backend does not expose the public Conversations resource. In ChatGPT auth mode,
the proxy therefore implements the complete
[OpenAI Conversations REST contract](https://developers.openai.com/api/reference/resources/conversations)
locally:

| Operation | Local endpoint |
| --- | --- |
| Create conversation | `POST /v1/conversations` |
| Retrieve conversation | `GET /v1/conversations/{conversation_id}` |
| Update conversation | `POST /v1/conversations/{conversation_id}` |
| Delete conversation | `DELETE /v1/conversations/{conversation_id}` |
| Create items | `POST /v1/conversations/{conversation_id}/items` |
| List items | `GET /v1/conversations/{conversation_id}/items` |
| Retrieve item | `GET /v1/conversations/{conversation_id}/items/{item_id}` |
| Delete item | `DELETE /v1/conversations/{conversation_id}/items/{item_id}` |

Pass a conversation ID to `POST /v1/responses` as either a string or `{ "id": "conv_..." }`.
The proxy prepends stored context to the upstream request, forces the backend-compatible
`store: false`, requests encrypted reasoning content for stateless continuation, then adds the new
input and completed output items to the local conversation. Responses JSON and SSE objects expose
the public `conversation: { "id": "..." }` field. Concurrent Responses calls for the same local
conversation are rejected with `409 Conflict` so their histories cannot race.

The default store is `responses_api_proxy_conversations.json` under the configured Codex SQLite
home and is written with mode `0600` on Unix. Conversation deletion retains its items internally,
as required by the public contract, while making the deleted conversation inaccessible.

Automatic compaction keeps a separate execution checkpoint without removing items from the
logical REST history. After 80 new items by default, the proxy tries `POST /responses/compact` for
the selected model. A successful compacted output becomes the context prefix for later calls; an
unsupported model/backend transparently continues with uncompacted context. Deleting an item
invalidates the checkpoint.

Example:

```shell
CONVERSATION_ID=$(curl -fsS http://127.0.0.1:60001/v1/conversations \
  -H 'Content-Type: application/json' \
  -d '{"metadata":{"topic":"demo"}}' | jq -r .id)

curl -N http://127.0.0.1:60001/v1/responses \
  -H 'Content-Type: application/json' \
  -d "{\"model\":\"gpt-5.5\",\"conversation\":\"${CONVERSATION_ID}\",\"input\":\"Hello\",\"stream\":true}"

curl -fsS "http://127.0.0.1:60001/v1/conversations/${CONVERSATION_ID}/items?order=asc"
```

Configuration and equivalent CLI overrides:

```toml
[responses_api_proxy]
listen_host = "127.0.0.1"
conversations_compat = true
conversation_store = "/absolute/path/conversations.json"
conversation_compact_after_items = 80 # 0 disables automatic compaction
```

## Chat Completions compatibility

ChatGPT auth mode exposes `POST /v1/chat/completions` as a compatibility adapter over the
Responses upstream. It follows the public
[OpenAI Chat Completions create contract](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create)
for the common text, image, function-tool, structured-output, reasoning, streaming, and usage
fields. Requests are converted to Responses input items; Responses JSON or SSE output is converted
back to `chat.completion` or `chat.completion.chunk` objects. Non-streaming calls are assembled
locally from an upstream event stream, so both `stream: false` and `stream: true` are available.

The adapter intentionally rejects parameters whose semantics cannot be preserved instead of
silently dropping them. It supports one choice per request (`n: 1`). Stored Chat Completions and
the associated list, retrieve, update, delete, and message-list operations are not exposed: the
ChatGPT Codex backend requires `store: false`.

Compatibility is enabled by default. Disable it for one invocation with:

```shell
codex responses-api-proxy --auth chatgpt --chat-completions-compat=false
```

Or persist the setting in `~/.codex/config.toml`:

```toml
[responses_api_proxy]
chat_completions_compat = false
```

The CLI flag takes precedence over the config file. Standard `-c` overrides work too, for example
`-c responses_api_proxy.chat_completions_compat=false`.

## ChatGPT authentication

ChatGPT mode uses the same configuration and authentication components as `codex app-server`:

- loads `$CODEX_HOME/config.toml` (normally `~/.codex/config.toml`);
- accepts the standard `-c key=value` configuration overrides and `--strict-config`;
- honors the configured OpenAI provider URL, headers, query parameters, system proxy policy,
  custom CA handling, and ChatGPT/Cloudflare cookies;
- refreshes managed OAuth credentials proactively and applies the normal reload/refresh recovery
  sequence after an upstream `401 Unauthorized`;
- replaces any inbound `Authorization` header with the managed ChatGPT credentials, including the
  ChatGPT account and FedRAMP routing headers when applicable.

The listener binds to `127.0.0.1` by default. Select another IPv4 or IPv6 address with
`--listen-host`; in ChatGPT auth mode, `responses_api_proxy.listen_host` provides the config-file
equivalent. The CLI flag takes precedence. The port is selected with `--port`; omitting it uses an
ephemeral port.

Binding to a non-loopback address exposes an endpoint with no downstream authentication. Any
reachable client can make requests through the configured upstream credentials. Restrict access
with host firewall rules or an authenticating reverse proxy; `0.0.0.0` listens on every IPv4
interface and `::` listens on every IPv6 interface.

## API key authentication

**IMPORTANT:** stdin authentication is designed to be run by a privileged user with access to
`OPENAI_API_KEY` so that an unprivileged user cannot inspect or tamper with the process. Though if
`--http-shutdown` is specified, an unprivileged user _can_ make a `GET` request to `/shutdown` to
shut down the server, as an unprivileged user could not send `SIGTERM` to kill the process.

A privileged user (i.e., `root` or a user with `sudo`) who has access to `OPENAI_API_KEY` would run the following to start the server, as `codex-responses-api-proxy` reads the auth token from `stdin`:

```shell
printenv OPENAI_API_KEY | env -u OPENAI_API_KEY codex-responses-api-proxy --http-shutdown --server-info /tmp/server-info.json
```

A non-privileged user would then run Codex as follows, specifying the `model_provider` dynamically:

```shell
PROXY_PORT=$(jq .port /tmp/server-info.json)
PROXY_BASE_URL="http://127.0.0.1:${PROXY_PORT}"
codex exec -c "model_providers.openai-proxy={ name = 'OpenAI Proxy', base_url = '${PROXY_BASE_URL}/v1', wire_api='responses' }" \
    -c model_provider="openai-proxy" \
    'Your prompt here'
```

When the unprivileged user was finished, they could shutdown the server using `curl` (since `kill -SIGTERM` is not an option):

```shell
curl --fail --silent --show-error "${PROXY_BASE_URL}/shutdown"
```

## Behavior

- With `--auth stdin`, reads the API key from `stdin`. All callers should pipe the key in (for
  example, `printenv OPENAI_API_KEY | codex-responses-api-proxy`).
- With `--auth chatgpt`, reads no secret from stdin and uses the managed Codex login.
- Formats the header value as `Bearer <key>` and attempts to `mlock(2)` the memory holding that header so it is not swapped to disk.
- Listens on the provided port or an ephemeral port if `--port` is not specified.
- Accepts the Responses resource operations listed above. Request bodies and headers are forwarded
  to the selected upstream, except that inbound `Authorization`, `Host`, and hop-by-hop headers are
  replaced or removed. Other requests receive `403`.
- Optionally writes a single-line JSON file with server info, currently `{ "port": <u16>, "pid": <u32> }`.
- Optionally writes request/response JSON dumps to a directory. Each accepted request gets a pair of files that share a sequence/timestamp prefix, for example `000001-1846179912345-request.json` and `000001-1846179912345-response.json`. Header values are dumped in full except `Authorization` and any header whose name includes `cookie`, which are redacted. Bodies are written as parsed JSON when possible, otherwise as UTF-8 text.
- Optional `--http-shutdown` enables `GET /shutdown` to terminate the process with exit code `0`. This allows one user (e.g., `root`) to start the proxy and another unprivileged user on the host to shut it down.

## CLI

```
codex-responses-api-proxy [--auth <stdin|chatgpt>] [-c <key=value>] [--strict-config] [--listen-host <IP>] [--port <PORT>] [--server-info <FILE>] [--http-shutdown] [--upstream-url <URL>] [--dump-dir <DIR>] [--chat-completions-compat <BOOL>] [--conversations-compat <BOOL>] [--conversation-store <FILE>] [--conversation-compact-after-items <COUNT>]
```

- `--auth <stdin|chatgpt>`: Selects stdin API-key auth (default) or the managed Codex ChatGPT login.
- `-c, --config <key=value>`: Overrides a value otherwise loaded from Codex `config.toml`.
- `--strict-config`: Fails when `config.toml` contains unknown fields.
- `--listen-host <IP>`: IPv4 or IPv6 socket address to bind. Defaults to `127.0.0.1`; hostnames are
  rejected. In ChatGPT mode this overrides `responses_api_proxy.listen_host` from `config.toml`.
- `--port <PORT>`: Port to bind. If omitted, an ephemeral port is chosen.
- `--server-info <FILE>`: If set, the proxy writes a single line of JSON with `{ "port": <PORT>, "pid": <PID> }` once listening.
- `--http-shutdown`: If set, enables `GET /shutdown` to exit the process with code `0`.
- `--upstream-url <URL>`: Absolute create-response URL ending in `/responses`. The default depends
  on `--auth`; resource subpaths and allowed query parameters are derived from this URL.
- `--dump-dir <DIR>`: If set, writes one request JSON file and one response JSON file per accepted proxy call under this directory. Filenames use a shared sequence/timestamp prefix so each pair is easy to correlate.
- `--chat-completions-compat <BOOL>`: Overrides the config-file setting for the local
  `POST /v1/chat/completions` adapter in ChatGPT auth mode. Defaults to `true`.
- `--conversations-compat <BOOL>`: Enables or disables the local Conversations and Conversation
  Items resources in ChatGPT auth mode. Defaults to `true`.
- `--conversation-store <FILE>`: Overrides the persistent local Conversations store path.
- `--conversation-compact-after-items <COUNT>`: Number of new items between opportunistic Compact
  checkpoints. Defaults to `80`; zero disables automatic compaction.
- Authentication is injected by the selected credential source; inbound `Authorization` is never
  forwarded.

For Azure, for example (ensure your deployment accepts `Authorization: Bearer <key>`):

```shell
printenv AZURE_OPENAI_API_KEY | env -u AZURE_OPENAI_API_KEY codex-responses-api-proxy \
  --http-shutdown \
  --server-info /tmp/server-info.json \
  --upstream-url "https://YOUR_PROJECT_NAME.openai.azure.com/openai/deployments/YOUR_DEPLOYMENT/responses?api-version=2025-04-01-preview"
```

## Notes

- All request headers for upstream Responses calls are forwarded aside from overriding
  `Authorization`, `Host`, and hop-by-hop headers. Response status and content-type are mirrored
  unless local Conversations or Chat Completions adaptation requires body translation.
- Conversations compatibility is local to ChatGPT auth mode; stdin API-key mode remains a strict
  upstream Responses proxy.

## Hardening Details

Care is taken to restrict access/copying to the value of `OPENAI_API_KEY` retained in memory:

- We leverage [`codex_process_hardening`](https://github.com/openai/codex/blob/main/codex-rs/process-hardening/README.md) so `codex-responses-api-proxy` is run with standard process-hardening techniques.
- At startup, we allocate a `1024` byte buffer on the stack and copy `"Bearer "` into the start of the buffer.
- We then read from `stdin`, copying the contents into the buffer after `"Bearer "`.
- After verifying the key matches `/^[a-zA-Z0-9_-]+$/` (and does not exceed the buffer), we create a `String` from that buffer (so the data is now on the heap).
- We zero out the stack-allocated buffer using https://crates.io/crates/zeroize so it is not optimized away by the compiler.
- We invoke `.leak()` on the `String` so we can treat its contents as a `&'static str`, as it will live for the rest of the process.
- On UNIX, we `mlock(2)` the memory backing the `&'static str`.
- When using the `&'static str` when building an HTTP request, we use `HeaderValue::from_static()` to avoid copying the `&str`.
- We also invoke `.set_sensitive(true)` on the `HeaderValue`, which in theory indicates to other parts of the HTTP stack that the header should be treated with "special care" to avoid leakage:

https://github.com/hyperium/http/blob/439d1c50d71e3be3204b6c4a1bf2255ed78e1f93/src/header/value.rs#L346-L376
