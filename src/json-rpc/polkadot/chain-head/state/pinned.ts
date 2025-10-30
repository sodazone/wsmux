import { getLogger } from "@logtape/logtape";

import type { UpstreamServer } from "../../../upstream";

const logger = getLogger(["wsmux", "chainhead", "unpin"]);

export function createPinnedBlocks() {
	const pinned = new Map();

	return {
		unfollow(upstreamSubId: string) {
			pinned.delete(upstreamSubId);
		},
		unsubscribeLocal(upstream: UpstreamServer, localId: string) {
			const follow = upstream.subscriptions
				.get("chainHead_v1_follow")
				?.getByLocalId(localId);

			if (follow) {
				const upstreamId = follow.upstreamSubId;
				const stillPinned = pinned.get(upstreamId);

				if (!stillPinned) {
					return;
				}

				for (const [hash, set] of stillPinned.entries()) {
					set.delete(localId);

					if (set.size === 0) {
						logger.info`[${upstreamId}] unpinned block ${hash} (unsubscribe)`;

						stillPinned.delete(hash);

						upstream.send({
							jsonrpc: "2.0",
							method: "chainHead_v1_unpin",
							params: [upstreamId, hash],
						});
					}
				}
				if (stillPinned.size === 0) {
					pinned.delete(upstreamId);
				}
			}
		},
		unpinLocal(upstream: UpstreamServer, localId: string, hashes: string[]) {
			const follow = upstream.subscriptions
				.get("chainHead_v1_follow")
				?.getByLocalId(localId);

			if (follow) {
				const upstreamId = follow.upstreamSubId;
				if (!pinned.has(upstreamId)) {
					pinned.set(upstreamId, new Map());
				}
				const stillPinned = pinned.get(upstreamId)!;
				hashes.forEach((hash) => {
					let set = stillPinned.get(hash);

					if (set) {
						for (const dead of [...set]) {
							if (!follow.hasLocalId(dead)) {
								set.delete(dead);
							}
						}
					} else {
						// First unpin means all locals were still pinned
						set = new Set(follow.getLocalIds());
						stillPinned.set(hash, set);
					}

					set.delete(localId);

					// If no one left pinned then real upstream unpin
					if (set.size === 0) {
						logger.debug((l) => l`[${upstreamId}] unpinned block ${hash}`);

						stillPinned.delete(hash);
						upstream.send({
							jsonrpc: "2.0",
							method: "chainHead_v1_unpin",
							params: [upstreamId, hash],
						});
					}
				});
				if (stillPinned.size === 0) {
					pinned.delete(upstreamId);
				}
			}
		},
	};
}
