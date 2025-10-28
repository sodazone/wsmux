import type { JSONRPCMethodHandler } from "../../methods";
import type { ChainHeadState } from "./state";

export const chainHead_v1_unpin = ({
	pinnedBlocks,
}: ChainHeadState): JSONRPCMethodHandler => {
	return {
		async handleRequest(upstream, _downstream, req) {
			const localId = req.params?.[0];
			if (!localId) return;
			const hashOrHashes = req.params[1];
			const hashes = Array.isArray(hashOrHashes)
				? hashOrHashes
				: [hashOrHashes];
			pinnedBlocks.pinLocal(upstream, localId, hashes);
		},
	};
};
