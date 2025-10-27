import { getLogger } from "@logtape/logtape";
import { filter, map } from "rxjs";

import { jsonRpcError } from "../../errors";
import type { JSONRPCMethodHandler } from "../../methods";
import type { JSONRPCNotification } from "../../types";
import { createSharedSubscription } from "../../upstream/shared";
import { createStateManager, type StateManager } from "./state";

const logger = getLogger("wsmux.chainhead.follow");
const MAX_FOLLOWS_PER_UPSTREAM = 2;

const creatingFollows = new Map<string, Promise<void>>();

async function getOrCreateFollow(
	followKey: string,
	createFn: () => Promise<void>,
) {
	if (!creatingFollows.has(followKey)) {
		const p = createFn();
		creatingFollows.set(followKey, p);
		try {
			await p;
			return true;
		} finally {
			creatingFollows.delete(followKey);
		}
	} else {
		await creatingFollows.get(followKey)!;
		return false;
	}
}

export const chainHead_v1_follow = (): JSONRPCMethodHandler => {
	function createStateMap() {
		const stateManagers = new Map<string, StateManager>();
		return {
			getOrCreate(key: string): StateManager {
				if (!stateManagers.has(key)) {
					const stateManager = createStateManager();
					stateManagers.set(key, stateManager);
				}
				return stateManagers.get(key)!;
			},
			remove(key: string) {
				stateManagers.delete(key);
			},
		};
	}
	const state = createStateMap();

	return {
		async handleRequest(upstream, downstream, request) {
			const methodKey = "chainHead_v1_follow";
			const clientId = downstream.clientId;

			logger.info((l) => l`Follow request from ${clientId}`);

			const follows = Array.from(upstream.subscriptions.entries()).filter(
				([k]) => k.startsWith(methodKey),
			);

			let selected = follows.length
				? follows.reduce((a, b) =>
						a[1].subscribersCount() < b[1].subscribersCount() ? a : b,
					)
				: undefined;

			if (
				(!selected || selected[1].subscribersCount() > 0) &&
				follows.length < MAX_FOLLOWS_PER_UPSTREAM
			) {
				const followIndex = follows.length;
				const followKey = `${methodKey}:${followIndex}`;

				const created = await getOrCreateFollow(followKey, async () => {
					logger.info(
						(l) =>
							l`[Follow] Creating upstream follow ${followKey} (${followIndex + 1}/${MAX_FOLLOWS_PER_UPSTREAM})`,
					);
					try {
						const { result: upstreamSubId } = (await upstream.request({
							...request,
							params: [true],
						})) as { result: string };
						if (!upstreamSubId) throw new Error("No upstreamSubId in response");

						const snapshot = state.getOrCreate(followKey);

						const shared = createSharedSubscription(
							upstreamSubId,
							snapshot.withUpdate(
								upstream.message$.pipe(
									filter(
										(msg) =>
											"method" in msg &&
											msg.method === "chainHead_v1_followEvent" &&
											msg.params?.subscription === upstreamSubId,
									),
									map((msg) => msg as JSONRPCNotification),
								),
							),
							async () => {
								logger.info(
									(l) => l`[Follow] Unfollowed upstream ${upstreamSubId}`,
								);
								await upstream.request({
									jsonrpc: "2.0",
									method: "chainHead_v1_unfollow",
									params: [upstreamSubId],
								});
								upstream.subscriptions.delete(followKey);
								state.remove(followKey);
							},
						);

						upstream.subscriptions.set(followKey, shared);
						const localId = downstream.getLocalId(upstreamSubId);
						downstream.send({ ...request, result: localId });
						shared.subscribeLocal(localId, downstream);
						upstream.unsubscribers.set(localId, () =>
							shared.unsubscribeLocal(localId),
						);
						await snapshot.replay(downstream, localId);
						logger.info(
							(l) =>
								l`[Follow] New follow ${followKey} assigned to ${clientId}`,
						);
					} catch (err) {
						logger.error((l) => l`[Follow] Error creating follow: ${err}`);
						downstream.send(
							jsonRpcError({
								kind: "INTERNAL_ERROR",
								message: String(err),
								req: request,
							}),
						);
					}
				});
				if (!created) {
					downstream.send(
						jsonRpcError({
							kind: "RATE_LIMITED",
							message: "Backpressure for reassignment",
							req: request,
						}),
					);
				}
				return;
			}

			selected = follows.reduce((a, b) =>
				a[1].subscribersCount() < b[1].subscribersCount() ? a : b,
			);

			if (!selected) {
				downstream.send(
					jsonRpcError({
						kind: "RATE_LIMITED",
						message: "No available follows",
						req: request,
					}),
				);
				return;
			}

			const [followKey, shared] = selected;
			const snapshot = state.getOrCreate(followKey);
			const localId = downstream.getLocalId(shared.upstreamSubId);
			if (shared.hasLocalSubscription(localId)) return;

			logger.info(
				(l) =>
					l`[Reuse] Assigning ${clientId} to ${followKey} (${shared.subscribersCount()} subs)`,
			);
			downstream.send({
				jsonrpc: "2.0",
				id: request.id ?? null,
				result: localId,
			});
			await snapshot.replay(downstream, localId);
			shared.subscribeLocal(localId, downstream);
			upstream.unsubscribers.set(localId, () =>
				shared.unsubscribeLocal(localId),
			);
		},
	};
};

export const chainHead_v1_unfollow: JSONRPCMethodHandler = {
	async handleRequest(upstream, downstream, req) {
		const localId = req.params?.[0];
		if (!localId) return;
		upstream.unsubscribe(localId);
		logger.debug((l) => l`[Unfollow] Local ${localId} unsubscribed`);
		downstream.send({ jsonrpc: "2.0", id: req.id ?? null, result: null });
	},
};
