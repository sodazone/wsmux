import { getLogger } from "@logtape/logtape";
import { filter } from "rxjs";

import type { JSONRPCMethodHandler } from "../../methods";
import type { JSONRPCNotification, JSONRPCResponse } from "../../types";
import { isSuccess } from "../../util";
import { metrics } from "./metrics/subscribe.metrics";

const logger = getLogger(["wsmux", "polkadot", "legacy", "subscribe"]);

const DEFAULT_MAX_SUBSCRIPTIONS_PER_CLIENT = 5;

export const subscribeLegacy = (
	unsubscribeMethodName: string,
	maxPerClient = DEFAULT_MAX_SUBSCRIPTIONS_PER_CLIENT,
): JSONRPCMethodHandler => {
	const activeCounts = new Map<number, number>();

	return {
		handleRequest: async (upstream, downstream, req) => {
			const clientId = downstream.clientId;
			const current = activeCounts.get(clientId) ?? 0;

			if (current >= maxPerClient) {
				downstream.send({
					jsonrpc: "2.0",
					id: req.id ?? null,
					error: {
						code: -32000,
						message: `max legacy subscriptions per client reached (${maxPerClient})`,
					},
				});
				return;
			}

			const response = await upstream.request(req);

			if (isSuccess(response)) {
				if (!response?.result) return;

				logger.info(
					`[${req.method}:${response.result}] subscribe (${current})`,
				);

				const upstreamSubId = response.result;
				const localId = downstream.getLocalId(upstreamSubId);

				downstream.send({ ...response, result: localId });

				activeCounts.set(clientId, current + 1);

				const rxSub = upstream.message$
					.pipe(
						filter(
							(msg: JSONRPCResponse | JSONRPCNotification) =>
								!("id" in msg) && msg.params?.subscription === upstreamSubId,
						),
					)
					.subscribe((msg) => {
						const n = msg as JSONRPCNotification;
						n.params.subscription = localId;
						downstream.send({
							...msg,
							params: {
								...n.params,
								subscription: localId,
							},
						});
					});

				metrics.subscribe(upstream.url, req.method);

				upstream.setUnsubscriber(localId, () => {
					try {
						rxSub.unsubscribe();
					} catch {
						//
					}

					const currentCount = activeCounts.get(clientId) ?? 1;
					const nextCount = Math.max(0, currentCount - 1);
					if (nextCount === 0) {
						activeCounts.delete(clientId);
					} else {
						activeCounts.set(clientId, nextCount);
					}

					logger.info(
						`[${req.method}:${upstreamSubId}] unsubscribe (${nextCount})`,
					);
					upstream.send({
						jsonrpc: "2.0",
						id: upstream.nextId(),
						method: unsubscribeMethodName,
						params: [upstreamSubId],
					});

					metrics.unsubscribe(upstream.url, unsubscribeMethodName);
				});

				downstream.addCloseFn(() => {
					upstream.unsubscribe(localId);
				});
			} else {
				downstream.send(response);
			}
		},
	};
};
