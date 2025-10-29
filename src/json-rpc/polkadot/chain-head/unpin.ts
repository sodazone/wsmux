import type { JSONRPCMethodHandler } from "../../methods";
import { chainHeadStateFrom } from "./state";

export const chainHead_v1_unpin: JSONRPCMethodHandler = {
	handleRequest: async (upstream, _downstream, req) => {
		const localId = req.params?.[0];
		if (!localId) return;
		const hashOrHashes = req.params[1];
		const hashes = Array.isArray(hashOrHashes) ? hashOrHashes : [hashOrHashes];
		const { pinnedBlocks } = chainHeadStateFrom(upstream);
		pinnedBlocks.pinLocal(upstream, localId, hashes);
	},
};
