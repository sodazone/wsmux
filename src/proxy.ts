import { getLogger } from "@logtape/logtape";
import client from "prom-client";

import { getOpts } from "./cli/opts";
import { loadConfig } from "./config";
import type { JSONRPCContextData } from "./json-rpc";
import { createUpstreamRegistry, jsonRpcMiddleware } from "./json-rpc";
import { polkadotMethods } from "./json-rpc/polkadot";
import { initLogger } from "./logger";
import { startJscMetrics } from "./runtime/metrics";
import { createWebSocketHandler } from "./ws";
import { metricsMiddleware } from "./ws/metrics";
import { rateLimiterMiddleware } from "./ws/rate-limiter";

async function run(
	options: Bun.Serve.HostnamePortServeOptions<JSONRPCContextData> = {},
) {
	const config = await loadConfig();
	await initLogger(config.logLevel);

	const logger = getLogger("wsmux");

	const registry = createUpstreamRegistry(config.upstream);
	await registry.connectAll();

	const middlewares = [];

	if (config.rateLimit.enabled) {
		middlewares.push(rateLimiterMiddleware(config.rateLimit));
	}

	middlewares.push(
		metricsMiddleware(),
		jsonRpcMiddleware(registry, polkadotMethods()),
	);

	const handler = createWebSocketHandler<JSONRPCContextData>({
		middlewares,
	});

	startJscMetrics();

	const server = Bun.serve({
		...options,
		async fetch(req, server) {
			if (new URL(req.url).pathname === "/metrics") {
				const metrics = await client.register.metrics();
				return new Response(metrics, {
					headers: { "Content-Type": client.register.contentType },
				});
			}

			if (server.upgrade(req, { data: { headers: req.headers } })) return;
			return new Response("Upgrade required", { status: 426 });
		},
		websocket: handler,
	});

	logger.info`Server listening on [${server.hostname}]:${server.port}`;

	let stopping = false;
	const shutdown = () => {
		if (stopping) return;
		stopping = true;
		handler.closeAll();
		server.stop();
		registry.destroy();
	};

	process.on("SIGINT", shutdown);
	process.on("SIGTERM", shutdown);

	return server;
}

export const Server = {
	run,
};

const opts = getOpts();
await Server.run({
	...opts.listen,
});
