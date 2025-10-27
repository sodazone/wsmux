import { getLogger } from "@logtape/logtape";

import type { JSONRPCMethodHandler } from "../../methods";

const logger = getLogger("wsmux.chainhead.unfollow");

export const chainHead_v1_unfollow: JSONRPCMethodHandler = {
	async handleRequest(upstream, downstream, req) {
		const localId = req.params?.[0];
		if (!localId) return;
		upstream.unsubscribe(localId);
		logger.debug((l) => l`Local ${localId} unsubscribed`);
		downstream.send({ jsonrpc: "2.0", id: req.id ?? null, result: null });
	},
};
