import type { JSONRPCMethodHandler } from "../methods";

export const forwardRequest: JSONRPCMethodHandler = {
	handleRequest: async (upstream, downstream, req) => {
		const response = await upstream.request(req);
		downstream.send(response);
	},
};
