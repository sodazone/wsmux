import { getLogger } from "@logtape/logtape";

import type { JSONRPCContextData } from "./json-rpc";
import { createUpstreamRegistry, jsonRpcMiddleware } from "./json-rpc";
import { polkadotMethods } from "./json-rpc/polkadot";
import { initLogger } from "./logger";
import { createWebSocketHandler } from "./ws";

const logger = getLogger("wsmux");

async function run(
	options: Bun.Serve.HostnamePortServeOptions<JSONRPCContextData> = {},
) {
	await initLogger();

	process.on("uncaughtException", (err) => {
		console.error("Uncaught Exception:", err);
	});
	process.on("unhandledRejection", (reason) => {
		console.error("Unhandled Rejection:", reason);
	});

	const registry = createUpstreamRegistry([
		{ url: "wss://dot-rpc.stakeworld.io" },
	]);
	await registry.connectAll();

	const handler = createWebSocketHandler<JSONRPCContextData>({
		middlewares: [jsonRpcMiddleware(registry, polkadotMethods)],
	});

	const server = Bun.serve({
		...options,
		fetch(req, server) {
			if (server.upgrade(req, { data: {} })) return;
			return new Response("Upgrade required", { status: 426 });
		},
		websocket: handler,
	});

	logger.info`Server listening on ${server.hostname}:${server.port}`;

	let stopping = false;
	const shutdown = () => {
		if (stopping) return;
		stopping = true;
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

await Server.run();
