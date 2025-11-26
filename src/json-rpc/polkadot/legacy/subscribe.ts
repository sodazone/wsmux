import { getLogger } from "@logtape/logtape";
import { filter } from "rxjs";

import type { JSONRPCMethodHandler } from "../../methods";
import type { JSONRPCNotification } from "../../types";
import { isSuccess } from "../../util";
import { metrics } from "./metrics/subscribe.metrics";

const logger = getLogger(["wsmux", "polkadot", "legacy", "subscribe"]);

const DEFAULT_MAX_SUBSCRIPTIONS_PER_CLIENT = 15;

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

        const rxSub = upstream.notification$
          .pipe(
            filter(
              (msg: JSONRPCNotification) =>
                msg.params?.subscription === upstreamSubId,
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

          upstream.send({
            jsonrpc: "2.0",
            id: upstream.nextId(),
            method: unsubscribeMethodName,
            params: [upstreamSubId],
          });

          metrics.unsubscribe(upstream.url, unsubscribeMethodName);
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
