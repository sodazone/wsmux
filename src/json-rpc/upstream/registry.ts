import { getLogger } from "@logtape/logtape";

import type { UpstreamConfig } from "../../config";
import type { JSONRPCContextData } from "../types";
import { createUpstreamPool } from "./pool";
import type { UpstreamRegistry, UpstreamServerAndPool } from "./types";

const logger = getLogger(["wsmux", "upstream", "registry"]);

export function createUpstreamRegistry(
	config: UpstreamConfig,
): UpstreamRegistry {
	const pools = config.servers.map(createUpstreamPool);
	const clientUpstream = new Map<number, UpstreamServerAndPool>();

	logger.info`#upstreamPools=${pools.length}`;

	// Just round-robin
	let lastPoolIndex = -1;

	const pickServer = (method: string): UpstreamServerAndPool | undefined => {
		const candidates = pools.filter((p) => p.supportsMethod(method));
		if (candidates.length === 0) return;

		lastPoolIndex = (lastPoolIndex + 1) % candidates.length;
		const pool = candidates[lastPoolIndex];

		if (pool) {
			const server = pool.acquire();
			if (!server) return undefined;

			return { pool, server };
		}
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
			const { pool, server } = clientUpstream.get(clientId)!;
			if (server.isReady()) return { pool, server };

			clientUpstream.delete(clientId);
			pool.release(server);
		}

		const poolAndServer = pickServer(method);
		if (poolAndServer === undefined) return undefined;

		clientUpstream.set(clientId, poolAndServer);

		return poolAndServer;
	};

	return {
		pools,
		resolveUpstream,
		disconnect: (clientId: number) => {
			const serverAndPool = clientUpstream.get(clientId);
			if (serverAndPool) {
				const { pool, server } = serverAndPool;
				pool.release(server);
			}
			clientUpstream.delete(clientId);
		},
		connectAll: async () => {
			await Promise.all(
				pools.map(async (pool) => {
					const conns = pool.start();
					if (!conns) {
						logger.warn("Cannot acquire server connection from pool");
						return;
					}

					const results = await Promise.allSettled(
						conns.map((conn) => conn.waitForReady()),
					);
					results.forEach((result) => {
						if (result.status === "rejected") {
							logger.warn(`Failed to connect to server: ${result.reason}`);
						}
					});
				}),
			);
		},
		destroy: () => {
			pools.forEach((pool) => {
				pool.stop();
			});
		},
	};
}
