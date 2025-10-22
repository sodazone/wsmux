import type { JSONRPCContextData } from "../types";
import { createUpstreamServer } from "./server";
import type { UpstreamRegistry } from "./types";

export function createUpstreamRegistry(
	configs: {
		url: string;
		supportedMethods?: string[];
	}[],
): UpstreamRegistry {
	const servers = configs.map(createUpstreamServer);

	const resolveUpstream = (_ctx: JSONRPCContextData, method: string) => {
		const server = servers.find(
			(s) =>
				s.isReady() &&
				(s.supportedMethods === undefined ||
					s.supportedMethods.includes(method)),
		);

		if (!server) return undefined;

		return server;
	};

	return {
		servers,
		resolveUpstream,
		connectAll: async () => {
			await Promise.all(servers.map((server) => server.connect()));
		},
		destroy: () => {
			servers.forEach((server) => {
				server.destroy();
			});
		},
	};
}
