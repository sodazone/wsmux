# wsmux

> [!NOTE]
> 🐉 Experimental / pre-alpha software. API and behavior may change without notice. 🐉

The **wsmux** server is a high-performance WebSocket multiplexer built on [Bun.serve](https://bun.com/docs/runtime/http/websockets), supporting [Polkadot JSON-RPC V1](https://paritytech.github.io/json-rpc-interface-spec/introduction.html) and technologies dependent on legacy interfaces.

It multiplexes client sessions over shared streams, with nested operation isolation, per-client subscription management, caching, telemetry, and server-affinity routing.

## Configuration

All settings (upstream servers, caching, rate limits, etc.) are defined in a YAML configuration file.

An example configuration is provided here:

[./wsmux.config.yaml](https://github.com/sodazone/wsmux/blob/main/wsmux.config.yaml)

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
