import { getLogger } from "@logtape/logtape";
import { filter } from "rxjs";

import type { JSONRPCMethodHandler } from "../../methods";
import type { JSONRPCNotification, JSONRPCResponse } from "../../types";
import { isSuccess } from "../../util";

const logger = getLogger(["wsmux", "polkadot", "legacy", "subscribe"]);

// TODO handle unsubscribe explicit verb in a separate method handler
export const subscribeLegacy = (
	unsubscribeMethodName: string,
): JSONRPCMethodHandler => {
	return {
		handleRequest: async (upstream, downstream, req) => {
			const response = await upstream.request(req);

			if (isSuccess(response)) {
				if (!response?.result) return;

				logger.info(`[${req.method}:${response.result}] subscribe`);

				const upstreamSubId = response.result;
				const localId = downstream.getLocalId(upstreamSubId);

				downstream.send({ ...response, result: localId });

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

				upstream.setUnsubscriber(localId, () => {
					rxSub.unsubscribe();

					logger.info(`[${req.method}:${upstreamSubId}] unsubscribe`);
					upstream.send({
						jsonrpc: "2.0",
						id: upstream.nextId(),
						method: unsubscribeMethodName,
						params: [upstreamSubId],
					});
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
