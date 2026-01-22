# wsmux

> [!WARNING]
> 🐉 Experimental / pre-alpha software. Behavior may change without notice. 🐉

The **wsmux** server is a high-performance WebSocket multiplexer built on [Bun.serve](https://bun.com/docs/runtime/http/websockets), supporting [Polkadot JSON-RPC V1](https://paritytech.github.io/json-rpc-interface-spec/introduction.html) and technologies dependent on legacy interfaces.

It multiplexes client sessions over shared streams, with nested operation isolation, per-client subscription management, caching, telemetry, and server-affinity routing.

## Architecture

### Caching

wsmux supports an optional cross-upstream cache to reduce redundant JSON-RPC requests and upstream load.

```yaml
cache:
  enabled: true
```

When disabled, all requests bypass the cache.

Caching is configured per method:

```yaml
cache:
  methods:
    chainHead_v1_header:
      enabled: true    # turn caching on/off for the method
      max_entries: 100 # maximum number of cached responses retained
```

For multiplexed methods like `chainHead_v1_*`, caching is applied per sequence of operations, not per raw request.
A cache entry consists of the initial `started` response plus the ordered stream of operation notifications. New clients replay the cached sequence and, if the operation is still active, attach to the live stream.

Although operations are shared, each client receives a filtered, rewritten view of the notifications (per-client subscription IDs), preserving logical isolation while reusing upstream work.

### Multiplexing

Conceptually, the multiplexing architecture looks like this:

```sh
Wsmux                            # load balancing: round-robin per upstream
├─ Upstream A                    # max_connections, WS load balancing: least-connections
│   ├─ WS-1                      # max_clients_per_connection
│   │   ├─ SharedSubscription-1  # e.g. max_subscribers for chainHead_v1_follow
│   │   │   ├─ Client-1
│   │   │   └─ Client-2
│   │   └─ SharedSubscription-2
│   │       └─ Client-3
│   └─ WS-2
│       └─ SharedSubscription-1
│           └─ Client-4
└─ Upstream B ...
```

<dl>
  <dt>Wsmux</dt>
  <dd>The entry point that distributes client connections across upstreams using round-robin.</dd>
  <dt>Upstream</dt>
  <dd>Backend servers. Each maintains a maximum connection count and uses least-connections load balancing across its WebSocket connections.</dd>
  <dt>WS</dt>
  <dd>A single WebSocket connection to an upstream. Connections dynamically scale between a minimum and maximum defined by <code>min/max_connections</code>. The <code>max_clients_per_connection</code> setting limits the number of clients each connection serves.</dd>
  <dt>SharedSubscription</dt>
  <dd>Multiplexed subscription streams (e.g. <code>chainHead_v1_follow</code>), shared by multiple clients up to the configured <code>max_subscribers</code>.</dd>
  <dt>Client</dt>
  <dd>An individual client session. Fully isolated logically but sharing the underlying WebSocket and subscription streams with other clients.</dd>
</dl>

## Configuration

All settings (upstream servers, caching, rate limits, etc.) are defined in a YAML configuration file.

An example configuration is provided here: [./wsmux.config.yaml](https://github.com/sodazone/wsmux/blob/main/wsmux.config.yaml)

### Presets

Upstreams can reference a preset to enable a predefined set of JSON-RPC methods.

Presets are convenience bundles that define which methods an upstream is allowed to expose, combining legacy and V1 APIs as needed.

Available presets:

<dl>
    <dt>polkadot_legacy</dt>
    <dd>Enables legacy Polkadot JSON-RPC methods (e.g. chain_*, state_*, author_*, system_*, subscriptions).</dd>
    <dt>polkadot_v1</dt>
    <dd>Enables Polkadot JSON-RPC V1 methods, including chainHead_v1_*, archive_v1_*, transaction_v1_*, and chainSpec_v1_*.</dd>
    <dt>polkadot</dt>
    <dd>A superset combining both polkadot_legacy and polkadot_v1.</dd>
</dl>

Example usage:

```yaml
upstream:
  servers:
    - name: stakeworld
      url: wss://dot-rpc.stakeworld.io
      presets: polkadot
```

Presets act as an allowlist: only methods included in the resolved preset may be forwarded to the upstream.

### Non-Preset Methods Fallback

Requests for methods not included in the upstream's preset are forwarded directly.

* wsmux assigns a temporary upstream ID to the request to avoid collisions between multiple clients
* The upstream response is rewritten to use the original client ID before sending back
* This works for standard requests, but not for subscriptions, which require dedicated multiplexing

This allows clients to call any method in a request-response fashion, even if it's not part of a preset.

## Development

To install dependencies:

```bash
bun install
```

To run the server:

```bash
bun start
```

## Docker

To build the Docker image:

```bash
docker build -t wsmux .
```

To run the Docker container:

```bash
docker run -it --rm \
  -p 8181:8181 \
  -v $(pwd)/wsmux.config.yaml:/usr/src/app/wsmux.config.yaml:ro \
  -e WSMUX_CONFIG=/usr/src/app/wsmux.config.yaml \
  -e WSMUX_LISTEN=:8181 \
  wsmux
```

---

Developed by [SO/DA zone](https://soda.zone).

❤️ Enjoy wsmux!

