import { getLogger } from "@logtape/logtape";

import type { JSONRPCMethodHandler } from "../../methods";

const logger = getLogger(["wsmux", "chainhead", "unfollow"]);

export const chainHead_v1_unfollow: JSONRPCMethodHandler = {
	async handleRequest(upstream, downstream, req) {
		const localId = req.params?.[0];
		if (!localId) return;

		logger.info((l) => l`[${upstream.url}:${localId}] chainHead_v1_unfollow`);

		upstream.unsubscribe(localId);
		downstream.send({ jsonrpc: "2.0", id: req.id ?? null, result: null });
	},
};
