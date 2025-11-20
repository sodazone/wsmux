import { getLogger } from "@logtape/logtape";
import { filter, takeWhile } from "rxjs";

import type { JSONRPCMethodHandler } from "../../methods";
import type { JSONRPCNotification, JSONRPCResponse } from "../../types";
import { isSuccess } from "../../util";
import { metrics } from "./metrics/storage.metrics";

const logger = getLogger(["wsmux", "polkadot", "legacy", "subscribe"]);

const DEFAULT_MAX_SUBSCRIPTIONS_PER_CLIENT = 50;

export const archive_v1_storage = (
	maxPerClient = DEFAULT_MAX_SUBSCRIPTIONS_PER_CLIENT,
): JSONRPCMethodHandler => {
	return storageSubscription(
		"archive_v1_storageEvent",
		new Set(["storageDone"]),
		maxPerClient,
	);
};

export const archive_v1_storageDiff = (
	maxPerClient = DEFAULT_MAX_SUBSCRIPTIONS_PER_CLIENT,
): JSONRPCMethodHandler => {
	return storageSubscription(
		"archive_v1_storageDiffEvent",
		new Set(["storageDiffDone"]),
		maxPerClient,
	);
};

// TODO: not clear if worth/smart to merge with legacy subscriptions handling
// not for the time being
const storageSubscription = (
	eventName: string,
	terminalEvents: Set<string>,
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
						message: `max storage subscripton-like requests per client reached (${maxPerClient})`,
					},
				});
				return;
			}

			if (downstream.closed) return;

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
						takeWhile(
							(msg: JSONRPCResponse | JSONRPCNotification) =>
								!("method" in msg) ||
								msg.method !== eventName ||
								!terminalEvents.has(msg.params?.result?.event),
							true,
						),
					)
					.subscribe({
						next: (msg) => {
							if (downstream.closed) return;
							const n = msg as JSONRPCNotification;
							downstream.send({
								...msg,
								params: { ...n.params, subscription: localId },
							});
						},
						error: () => cleanup(),
						complete: () => cleanup(),
					});

				metrics.subscribe(upstream.url, req.method);

				let cleaned = false;
				const cleanup = () => {
					if (cleaned) return;
					cleaned = true;

					try {
						rxSub.unsubscribe();
					} catch {}

					const cnt = activeCounts.get(clientId) ?? 1;
					const next = Math.max(0, cnt - 1);
					if (next === 0) activeCounts.delete(clientId);
					else activeCounts.set(clientId, next);

					metrics.unsubscribe(upstream.url, req.method);
				};

				upstream.setUnsubscriber(localId, cleanup);

				downstream.addCloseFn(() => {
					upstream.unsubscribe(localId);
				});
			} else {
				downstream.send(response);
			}
		},
	};
};
