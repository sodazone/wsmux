import type { JSONRPCApexMethodHandler } from "../methods";
import { resolvePreset } from ".";

const rpc_methods = (methods: string[]): JSONRPCApexMethodHandler => {
	return {
		handleRequest: async (downstream, req) => {
			downstream.send({
				jsonrpc: "2.0",
				id: req.id ?? null,
				result: {
					methods: Array.from(methods),
				},
			});
		},
	};
};

export const resolveRoutingPreset = (
	presetName: string,
): Record<string, JSONRPCApexMethodHandler> => {
	if (presetName === "polkadot") {
		const methods = resolvePreset("polkadot");
		return {
			rpc_methods: rpc_methods(methods),
		};
	}
	throw new Error(`Unknown preset: ${presetName}`);
};
