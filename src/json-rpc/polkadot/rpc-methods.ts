import type { JSONRPCMethodHandler } from "../methods";

export const rpc_methods: JSONRPCMethodHandler = {
	handleRequest: async (upstream, downstream, req) => {
		if (upstream.supportedMethods) {
			downstream.send({
				jsonrpc: "2.0",
				id: req.id ?? null,
				result: {
					methods: Array.from(upstream.supportedMethods),
				},
			});
		} else {
			const response = await upstream.request(req);
			downstream.send(response);
		}
	},
};
