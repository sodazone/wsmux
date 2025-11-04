import { getLogger } from "@logtape/logtape";

import type {
	SharedSubscription,
	SharedSubscriptionPool,
	UpstreamServer,
} from "../../../upstream";
import { createStateManager, type StateManager } from "./manager";
import { createPinnedBlocks } from "./pinned";

const logger = getLogger(["wsmux", "chainhead", "state"]);

function createStateManagersMap(onUnfollow: (upstreamSubId: string) => void) {
	const stateManagers = new Map<string, StateManager>();
	return {
		get(key: string) {
			return stateManagers.get(key);
		},
		createSharedSubscription(
			followKey: string,
			upstream: UpstreamServer,
			upstreamSubId: string,
			pool: SharedSubscriptionPool,
		): SharedSubscription {
			if (!stateManagers.has(followKey)) {
				const stateManager = createStateManager();
				stateManagers.set(followKey, stateManager);
			}
			const stateManager = stateManagers.get(followKey)!;
			const cleanup = async (unfollow = true) => {
				logger.info(
					(l) =>
						l`[${upstreamSubId}] ${unfollow ? "unfollow" : "clean up"} upstream`,
				);
				if (unfollow) {
					await upstream.request({
						jsonrpc: "2.0",
						method: "chainHead_v1_unfollow",
						params: [upstreamSubId],
					});
				}
				stateManagers.delete(followKey);
				onUnfollow(upstreamSubId);
			};
			return pool.createSharedSubscription(
				followKey,
				upstreamSubId,
				stateManager.withUpdater(upstreamSubId, upstream, () => {
					// we send up the unfollow, seemingly some RPCs expect that
					// after stopping
					void cleanup(true);
					pool.remove(followKey);
				}),
				cleanup,
			);
		},
	};
}

export type ChainHeadState = ReturnType<typeof createChainHeadState>;

function createChainHeadState() {
	const pinnedBlocks = createPinnedBlocks();
	const managers = createStateManagersMap((upstreamSubId) => {
		pinnedBlocks.unfollow(upstreamSubId);
	});
	return {
		managers,
		pinnedBlocks,
	};
}

export function chainHeadStateFrom(upstream: UpstreamServer) {
	return upstream.getOrCreateState("chainHead", createChainHeadState);
}
