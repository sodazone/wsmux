import { getLogger } from "@logtape/logtape";

import type { JSONRPCMethodHandler } from "../methods";

const logger = getLogger(["wsmux", "chainhead", "ignore"]);

export const ignoreRequest: JSONRPCMethodHandler = {
	handleRequest: async (upstream, downstream, req) => {
		logger.info(
			`[${upstream.url}:${downstream.clientId}] ignore method=${req.method} id=${req.id}`,
		);
	},
};
