# wsmux

> [!WARNING]
> 🐉 Experimental / pre-alpha software. Behavior may change without notice. 🐉

The **wsmux** server is a high-performance WebSocket multiplexer built on [Bun.serve](https://bun.com/docs/runtime/http/websockets), supporting [Polkadot JSON-RPC V1](https://paritytech.github.io/json-rpc-interface-spec/introduction.html) and technologies dependent on legacy interfaces.

It multiplexes client sessions over shared streams, with nested operation isolation, per-client subscription management, caching, telemetry, and server-affinity routing.

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

## Development

To install dependencies:

```bash
bun install
```

To run the server:

```bash
bun start
```

---

Enjoy wsmux!
