import { getLogger } from "@logtape/logtape";

import type { UpstreamServer } from "../../../upstream";

const logger = getLogger(["wsmux", "chainhead", "unpin"]);

const MAX_HASHES_PER_UPSTREAM = 512;

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
				const upstreamSubId = follow.upstreamSubId;
				const stillPinned = pinned.get(upstreamSubId);

				if (!stillPinned) {
					return;
				}

				for (const [hash, set] of stillPinned.entries()) {
					set.delete(localId);

					if (set.size === 0) {
						logger.info`[${upstreamSubId}] unpinned block ${hash} (unsubscribe)`;

						stillPinned.delete(hash);

						upstream.send({
							jsonrpc: "2.0",
							method: "chainHead_v1_unpin",
							params: [upstreamSubId, hash],
						});
					}
				}
				if (stillPinned.size === 0) {
					pinned.delete(upstreamSubId);
				}
			}
		},
		unpinLocal(upstream: UpstreamServer, localId: string, hashes: string[]) {
			const follow = upstream.subscriptions
				.get("chainHead_v1_follow")
				?.getByLocalId(localId);

			if (follow) {
				const upstreamSubId = follow.upstreamSubId;
				if (!pinned.has(upstreamSubId)) {
					pinned.set(upstreamSubId, new Map());
				}
				const stillPinned = pinned.get(upstreamSubId)!;
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

						if (stillPinned.size >= MAX_HASHES_PER_UPSTREAM) {
							const oldestHash = stillPinned.keys().next().value;
							stillPinned.delete(oldestHash);
						}
					}

					set.delete(localId);

					// If no one left pinned then real upstream unpin
					if (set.size === 0) {
						logger.debug((l) => l`[${upstreamSubId}] unpinned block ${hash}`);

						stillPinned.delete(hash);
						upstream.send({
							jsonrpc: "2.0",
							method: "chainHead_v1_unpin",
							params: [upstreamSubId, hash],
						});
					}
				});
				if (stillPinned.size === 0) {
					pinned.delete(upstreamSubId);
				}
			}
		},
	};
}
