import { getLogger } from "@logtape/logtape";
import { filter, map } from "rxjs";

import { jsonRpcError } from "../../errors";
import type { JSONRPCMethodHandler } from "../../methods";
import type { JSONRPCNotification } from "../../types";
import { createSharedSubscription } from "../../upstream/shared";
import { deferred } from "../../util";
import { createStateManager, type StateManager } from "./state";

const logger = getLogger("wsmux.chainhead.follow");

export const chainHead_v1_follow: JSONRPCMethodHandler = {
	handleRequest: async (upstream, downstream, request) => {
		const methodKey = "chainHead_v1_follow";
		let snapshot = upstream.state[methodKey] as StateManager;
		if (!snapshot) {
			snapshot = createStateManager();
			upstream.state[methodKey] = snapshot;
		}

		logger.debug(
			(l) =>
				l`Handling ${methodKey} request from downstream ${downstream.clientId}`,
		);

		const inflight = upstream.pending.get(methodKey);
		if (inflight) {
			logger.debug((l) => l`Waiting for inflight ${methodKey} setup`);
			await inflight;
		}

		// reuse existing upstream

		if (upstream.subscriptions.has(methodKey)) {
			const shared = upstream.subscriptions.get(methodKey);
			if (shared) {
				const localId = downstream.getLocalId(shared.upstreamSubId);

				if (shared.hasLocalSubscription(localId)) {
					return;
				}

				logger.info`Reusing existing upstream subscription ${shared.upstreamSubId} for downstream ${downstream.clientId}`;

				downstream.send({
					jsonrpc: "2.0",
					id: request.id ?? null,
					result: localId,
				});

				logger.debug(
					(l) =>
						l`Replaying snapshot for downstream ${downstream.clientId} localId=${localId}`,
				);

				await snapshot.replay(downstream, localId);

				shared.subscribeLocal(localId, downstream);
				upstream.unsubscribers.set(localId, () =>
					shared.unsubscribeLocal(localId),
				);

				logger.debug(
					(l) =>
						l`Downstream ${downstream.clientId} subscribed to shared ${shared.upstreamSubId}`,
				);
			}
			return;
		}

		// create new upstream follow

		const pending = deferred();
		upstream.pending.set(methodKey, pending.promise);

		try {
			logger.info`Creating new upstream follow for ${methodKey}`;

			const response = await upstream.request({ ...request, params: [true] });
			const upstreamSubId = response.result as string;
			if (upstreamSubId == null) {
				logger.error("Failed to create upstream subscription", {
					request,
					response,
				});
				throw new Error("Failed to create upstream subscription");
			}

			logger.info`New upstream subscription ${upstreamSubId} established`;

			const source$ = upstream.message$.pipe(
				filter(
					(msg) =>
						"method" in msg &&
						msg.method === "chainHead_v1_followEvent" &&
						msg.params?.subscription === upstreamSubId,
				),
				map((msg) => msg as JSONRPCNotification),
			);

			const sharedSubscription = createSharedSubscription(
				upstreamSubId,
				snapshot.withUpdate(source$),
				() => {
					logger.info`Unfollowing upstream subscription ${upstreamSubId}`;

					upstream.send({
						jsonrpc: "2.0",
						method: "chainHead_v1_unfollow",
						params: [upstreamSubId],
					});

					upstream.subscriptions.delete(methodKey);
				},
			);

			upstream.subscriptions.set(methodKey, sharedSubscription);

			const localId = downstream.getLocalId(sharedSubscription.upstreamSubId);

			// send initial response
			downstream.send({ ...response, result: localId });

			// subscribe downstream
			sharedSubscription.subscribeLocal(localId, downstream);
			logger.debug(
				(l) =>
					l`Downstream ${downstream.clientId} subscribed to shared upstream ${upstreamSubId}`,
			);

			// hook clean up function by local id
			upstream.unsubscribers.set(localId, () => {
				logger.debug(
					(l) =>
						l`Unsubscribing localId=${localId} for downstream ${downstream.clientId}`,
				);

				sharedSubscription.unsubscribeLocal(localId);
			});
			pending.resolve();
			logger.debug((l) => l`${methodKey} setup complete`);
		} catch (err) {
			pending.reject(err);
			downstream.send(
				jsonRpcError({
					kind: "INTERNAL_ERROR",
					message: String(err),
					req: request,
				}),
			);
		} finally {
			upstream.pending.delete(methodKey);
		}
	},
};

export const chainHead_v1_unfollow: JSONRPCMethodHandler = {
	handleRequest: async (upstream, downstream, req) => {
		const localId = req.params?.[0];
		if (!localId) return;

		upstream.unsubscribe(localId);

		downstream.send({
			jsonrpc: "2.0",
			id: req.id ?? null,
			result: null,
		});
	},
};
