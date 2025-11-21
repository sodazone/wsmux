import { filter, takeWhile } from "rxjs";

import type { JSONRPCMethodHandler } from "@/json-rpc/methods";
import { isSuccess } from "../../util";
import { metrics } from "./metrics/subscription.metrics";

type SubscriptionBehavior = {
	matchEvent: (msg: any, upstreamId: string) => boolean;
	isTerminal: (msg: any) => boolean;
	rewriteEvent: (msg: any, localId: string) => any;
};

type SubscriptionQuota = ReturnType<typeof createSubscriptionQuota>;

const DEFAULT_MAX_SUBSCRIPTIONS_PER_CLIENT = 50;

function createSubscriptionQuota(maxPerClient?: number) {
	const _maxPerClient = maxPerClient ?? DEFAULT_MAX_SUBSCRIPTIONS_PER_CLIENT;
	const _activeCounts = new Map<number, number>();
	return {
		maxPerClient: _maxPerClient,
		increment: (clientId: number) => {
			_activeCounts.set(clientId, (_activeCounts.get(clientId) ?? 0) + 1);
		},
		decrement: (clientId: number) => {
			const cur = _activeCounts.get(clientId) ?? 1;
			const next = cur - 1;
			next > 0
				? _activeCounts.set(clientId, next)
				: _activeCounts.delete(clientId);
		},
		hasReachedMax: (clientId: number) => {
			return (_activeCounts.get(clientId) ?? 0) >= _maxPerClient;
		},
	};
}

const createSubscriptionHandler = (
	behavior: SubscriptionBehavior,
	quota: SubscriptionQuota,
): JSONRPCMethodHandler => {
	return {
		handleRequest: async (upstream, downstream, req) => {
			const { clientId } = downstream;

			if (quota.hasReachedMax(clientId)) {
				downstream.send({
					jsonrpc: "2.0",
					id: req.id ?? null,
					error: {
						code: -32000,
						message: `max subscription requests per client reached (${quota.maxPerClient})`,
					},
				});
				return;
			}

			if (downstream.closed) return;

			const resp = await upstream.request(req);
			if (!isSuccess(resp)) {
				downstream.send(resp);
				return;
			}

			const upstreamId = resp.result;
			const localId = downstream.getLocalId(upstreamId);

			// Send initial response with local ID
			downstream.send({ ...resp, result: localId });

			quota.increment(clientId);

			metrics.subscribe(upstream.url, req.method);

			let done = false;

			const sub = upstream.message$
				.pipe(
					filter((msg) => behavior.matchEvent(msg, upstreamId)),
					takeWhile((msg) => !behavior.isTerminal(msg), true),
				)
				.subscribe({
					next: (msg) => {
						if (!done && !downstream.closed) {
							downstream.send(behavior.rewriteEvent(msg, localId));
						}
					},
					error: () => cleanup(),
					complete: () => cleanup(),
				});

			const cleanup = () => {
				if (done) return;
				done = true;

				try {
					sub.unsubscribe();
				} catch {}
				quota.decrement(clientId);
				metrics.unsubscribe(upstream.url, req.method);
			};

			upstream.setUnsubscriber(localId, cleanup);
			downstream.addCloseFn(() => upstream.unsubscribe(localId));
		},
	};
};

const defaultBehavior = (
	eventName: string,
	terminalEvents: Set<string>,
): SubscriptionBehavior => ({
	matchEvent: (msg, upstreamId) =>
		!("id" in msg) && msg.params?.subscription === upstreamId,
	isTerminal: (msg) =>
		"method" in msg &&
		msg.method === eventName &&
		terminalEvents.has(msg.params?.result?.event),
	rewriteEvent: (msg, localId) => ({
		...msg,
		params: { ...msg.params, subscription: localId },
	}),
});

export function createDefaultSubscriptionHandler(
	eventName: string,
	terminalEvents: string[],
	maxPerClient?: number,
): JSONRPCMethodHandler {
	const quota = createSubscriptionQuota(maxPerClient);

	return createSubscriptionHandler(
		defaultBehavior(eventName, new Set(terminalEvents)),
		quota,
	);
}
