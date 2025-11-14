import { getLogger } from "@logtape/logtape";

import type { JSONRPCMethodHandler } from "../../methods";

const logger = getLogger(["wsmux", "polkadot", "legacy", "unsubscribe"]);

export const unsubscribeLegacy: JSONRPCMethodHandler = {
	handleRequest: async (upstream, downstream, req) => {
		if (req.params && Array.isArray(req.params) && req.params.length > 0) {
			const localId = downstream.getLocalId(req.params[0]);
			logger.info`[${localId}] unsubscribing`;
			upstream.unsubscribe(localId);
		} else {
			// TODO: to debug log level...
			logger.warn`no subscription provided ${Bun.inspect(req)}`;
		}
	},
};
