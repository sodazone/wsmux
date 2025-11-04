import type { JSONRPCContextData } from "../types";
import { createUpstreamServer } from "./server";
import type { UpstreamRegistry, UpstreamServer } from "./types";

export function createUpstreamRegistry(
	configs: {
		url: string;
		supportedMethods?: string[];
	}[],
): UpstreamRegistry {
	const servers = configs.map(createUpstreamServer);
	const clientUpstream = new Map<number, UpstreamServer>();
	let lastIndex = -1;

	const resolveUpstream = (ctx: JSONRPCContextData, method: string) => {
		const clientId = ctx.client?.clientId;
		if (clientId === undefined) {
			return undefined;
		}

		if (clientUpstream.has(clientId)) {
			const server = clientUpstream.get(clientId)!;
			if (server.isReady()) return server;
			clientUpstream.delete(clientId);
		}

		const candidates = servers.filter(
			(s) =>
				s.isReady() &&
				(s.supportedMethods === undefined ||
					s.supportedMethods.includes(method)),
		);
		if (candidates.length === 0) return undefined;

		lastIndex = (lastIndex + 1) % candidates.length;
		const server = candidates[lastIndex]!;

		clientUpstream.set(clientId, server);
		return server;
	};

	return {
		servers,
		resolveUpstream,
		disconnect: (clientId: number) => {
			clientUpstream.delete(clientId);
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
