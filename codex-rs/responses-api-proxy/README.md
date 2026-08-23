# codex-responses-api-proxy

#### tl;dr:

Use the existing Codex ChatGPT login and expose it as a local Responses API:

```shell
codex responses-api-proxy --auth chatgpt --port 60001

curl http://127.0.0.1:60001/v1/responses \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpt-5.1-codex","input":"Hello","stream":true}'
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

The listener is always bound to `127.0.0.1`. The port is selected with `--port`; omitting it uses
an ephemeral port.

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
codex-responses-api-proxy [--auth <stdin|chatgpt>] [-c <key=value>] [--strict-config] [--port <PORT>] [--server-info <FILE>] [--http-shutdown] [--upstream-url <URL>] [--dump-dir <DIR>]
```

- `--auth <stdin|chatgpt>`: Selects stdin API-key auth (default) or the managed Codex ChatGPT login.
- `-c, --config <key=value>`: Overrides a value otherwise loaded from Codex `config.toml`.
- `--strict-config`: Fails when `config.toml` contains unknown fields.
- `--port <PORT>`: Port to bind on `127.0.0.1`. If omitted, an ephemeral port is chosen.
- `--server-info <FILE>`: If set, the proxy writes a single line of JSON with `{ "port": <PORT>, "pid": <PID> }` once listening.
- `--http-shutdown`: If set, enables `GET /shutdown` to exit the process with code `0`.
- `--upstream-url <URL>`: Absolute create-response URL ending in `/responses`. The default depends
  on `--auth`; resource subpaths and allowed query parameters are derived from this URL.
- `--dump-dir <DIR>`: If set, writes one request JSON file and one response JSON file per accepted proxy call under this directory. Filenames use a shared sequence/timestamp prefix so each pair is easy to correlate.
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

- Only `POST /v1/responses` is permitted. No query strings are allowed.
- All request headers are forwarded to the upstream call (aside from overriding `Authorization` and `Host`). Response status and content-type are mirrored from upstream.

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
