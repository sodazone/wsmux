import type { CacheConfig } from "@/config";
import type { JSONRPCMethodHandler } from "../methods";
import { archive_v1_body } from "./archive/body";
import { archive_v1_header } from "./archive/header";
import { archive_v1_storage, archive_v1_storageDiff } from "./archive/storage";
import { chainHead_v1_body } from "./chain-head/body";
import { chainHead_v1_call } from "./chain-head/call";
import { chainHead_v1_follow } from "./chain-head/follow";
import { chainHead_v1_forward } from "./chain-head/forward";
import { chainHead_v1_header } from "./chain-head/header";
import { isCacheEnabled } from "./chain-head/ops/util";
import { chainHead_v1_storage } from "./chain-head/storage";
import { chainHead_v1_unfollow } from "./chain-head/unfollow";
import { chainHead_v1_unpin } from "./chain-head/unpin";
import { forwardRequest } from "./forward";
import { ignoreRequest } from "./ignore";
import { subscribeLegacy } from "./legacy/subscribe";
import { unsubscribeLegacy } from "./legacy/unsubscribe";
import { rpc_methods } from "./rpc-methods";
import { transactionWatch_v1_submitAndWatch } from "./transaction/watch";

export function polkadotMethods(config: CacheConfig) {
	const opCacheEnabled = isCacheEnabled(config);

	return {
		rpc_methods,

		// V1
		chainHead_v1_follow: chainHead_v1_follow(),
		chainHead_v1_unfollow,
		chainHead_v1_unpin,
		chainHead_v1_header: chainHead_v1_header(config),
		chainHead_v1_storage: chainHead_v1_storage(config),
		chainHead_v1_body: chainHead_v1_body(config),
		chainHead_v1_call: chainHead_v1_call(config),
		chainHead_v1_continue: opCacheEnabled
			? ignoreRequest
			: chainHead_v1_forward,
		chainHead_v1_stopOperation: opCacheEnabled
			? ignoreRequest
			: chainHead_v1_forward,

		archive_v1_storage: archive_v1_storage(),
		archive_v1_storageDiff: archive_v1_storageDiff(),
		archive_v1_body: archive_v1_body(config),
		archive_v1_header: archive_v1_header(config),

		transactionWatch_v1_submitAndWatch: transactionWatch_v1_submitAndWatch(),

		// Legacy subscriptions
		chain_subscribeNewHead: subscribeLegacy("chain_unsubscribeNewHead"),
		chain_unsubscribeNewHead: unsubscribeLegacy,
		chain_subscribeNewHeads: subscribeLegacy("chain_unsubscribeNewHeads"),
		chain_unsubscribeNewHeads: unsubscribeLegacy,
		chain_subscribeFinalizedHeads: subscribeLegacy(
			"chain_unsubscribeFinalizedHeads",
		),
		chain_unsubscribeFinalizedHeads: unsubscribeLegacy,
		chain_subscribeFinalizedHead: subscribeLegacy(
			"chain_unsubscribeFinalizedHead",
		),
		chain_unsubscribeFinalizedHead: unsubscribeLegacy,
		state_subscribeRuntimeVersion: subscribeLegacy(
			"state_unsubscribeRuntimeVersion",
		),
		state_unsubscribeRuntimeVersion: unsubscribeLegacy,
		state_subscribeStorage: subscribeLegacy("state_unsubscribeStorage"),
		state_unsubscribeStorage: unsubscribeLegacy,

		// Fallback
		__fallback: forwardRequest,
	} as const satisfies Record<string, JSONRPCMethodHandler>;
}
