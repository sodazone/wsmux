import { getLogger } from "@logtape/logtape";

import type { UpstreamConfig } from "../../config";
import type { JSONRPCApexMethodHandler } from "../methods";
import { resolveRoutingPreset } from "../presets/routing";
import type { JSONRPCContextData } from "../types";
import { createUpstreamServer } from "./server";
import { startServerStats } from "./stats";
import type { UpstreamRegistry, UpstreamServer } from "./types";

const logger = getLogger(["wsmux", "upstream", "registry"]);

export function createUpstreamRegistry(
	config: UpstreamConfig,
): UpstreamRegistry {
	const servers = config.servers.map(createUpstreamServer);
	const clientUpstream = new Map<number, UpstreamServer>();
	const apex = Object.fromEntries(
		config.apex
			.map(resolveRoutingPreset)
			.flatMap((record) => Object.entries(record)),
	);

	logger.info(`#servers=${servers.length}, apex=${config.apex}`);

	let lastIndex = -1;

	if (config.debug.stats.enabled) {
		startServerStats(servers, config.debug.stats);
	}

	const pickServer = (method: string): UpstreamServer | undefined => {
		const candidates = servers.filter(
			(s) =>
				s.isReady() &&
				s.hasCapacity() &&
				(s.supportedMethods === undefined || s.supportedMethods.has(method)),
		);
		if (candidates.length === 0) return undefined;
		lastIndex = (lastIndex + 1) % candidates.length;
		return candidates[lastIndex];
	};

	const resolveApexMethod = (
		method: string,
	): JSONRPCApexMethodHandler | undefined => {
		return apex[method];
	};

	const resolveUpstream = (ctx: JSONRPCContextData, method: string) => {
		// by default methods are sticky
		const client = ctx.client;
		if (client === undefined) {
			return undefined;
		}

		const clientId = client.clientId;
		if (clientId === undefined) {
			return undefined;
		}

		if (clientUpstream.has(clientId)) {
			const server = clientUpstream.get(clientId)!;
			if (server.isReady()) return server;

			clientUpstream.delete(clientId);
			server.connections.dec();
		}

		const server = pickServer(method);
		if (server === undefined) return undefined;

		clientUpstream.set(clientId, server);
		server.connections.inc();

		return server;
	};

	return {
		servers,
		resolveApexMethod,
		resolveUpstream,
		disconnect: (clientId: number) => {
			const server = clientUpstream.get(clientId);
			if (server) {
				server.connections.dec();
			}
			clientUpstream.delete(clientId);
		},
		connectAll: async () => {
			await Promise.all(
				servers.map(async (server) => {
					await server.connect();
					try {
						await server.waitForReady();
					} catch (err: any) {
						if (err.name === "TimeoutError") {
							logger.warn(`Timeout waiting for ready: ${server.url}`);
						} else {
							throw err;
						}
					}
				}),
			);
		},
		destroy: () => {
			servers.forEach((server) => {
				server.stop();
			});
		},
	};
}
