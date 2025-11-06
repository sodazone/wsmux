import type { JSONRPCContextData } from "../types";
import { createUpstreamServer } from "./server";
import type {
	UpstreamRegistry,
	UpstreamServer,
	UpstreamServerConfig,
} from "./types";

export function createUpstreamRegistry(
	configs: UpstreamServerConfig[],
): UpstreamRegistry {
	const servers = configs.map(createUpstreamServer);
	const clientUpstream = new Map<number, UpstreamServer>();
	let lastIndex = -1;

	const resolveUpstream = (ctx: JSONRPCContextData, method: string) => {
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

		const candidates = servers.filter(
			(s) =>
				s.isReady() &&
				s.hasCapacity() &&
				(s.supportedMethods === undefined ||
					s.supportedMethods.includes(method)),
		);
		if (candidates.length === 0) return undefined;

		lastIndex = (lastIndex + 1) % candidates.length;
		const server = candidates[lastIndex]!;

		client.addCloseFn(() => {
			server.connections.dec();
		});
		clientUpstream.set(clientId, server);
		server.connections.inc();

		return server;
	};

	return {
		servers,
		resolveUpstream,
		disconnect: (clientId: number) => {
			clientUpstream.delete(clientId);
			const server = clientUpstream.get(clientId);
			if (server) {
				server.connections.dec();
			}
		},
		connectAll: async () => {
			await Promise.all(
				servers.map(async (server) => {
					await server.connect();
					await server.waitForReady();
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
