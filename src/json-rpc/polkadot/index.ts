import type { CacheConfig, MethodCacheConfig } from "@/config";
import type { JSONRPCMethodHandler } from "../methods";
import { chainHead_v1_body } from "./chain-head/body";
import { chainHead_v1_call } from "./chain-head/call";
import { chainHead_v1_follow } from "./chain-head/follow";
import { chainHead_v1_forward } from "./chain-head/forward";
import { chainHead_v1_header } from "./chain-head/header";
import { hasOpCacheEnabled } from "./chain-head/ops/util";
import { chainHead_v1_storage } from "./chain-head/storage";
import { chainHead_v1_unfollow } from "./chain-head/unfollow";
import { chainHead_v1_unpin } from "./chain-head/unpin";
import { forwardRequest } from "./forward";
import { ignoreRequest } from "./ignore";
import { subscribeLegacy } from "./legacy/subscribe";
import { unsubscribeLegacy } from "./legacy/unsubscribe";
import { rpc_methods } from "./rpc-methods";

export function polkadotMethods(config: CacheConfig) {
	const opCacheEnabled = hasOpCacheEnabled(config);

	return {
		rpc_methods,

		// V1
		chainHead_v1_follow: chainHead_v1_follow(),
		chainHead_v1_unfollow,
		chainHead_v1_unpin,
		chainHead_v1_header: withCacheOrForward(config, chainHead_v1_header),
		chainHead_v1_storage: withCacheOrForward(config, chainHead_v1_storage),
		chainHead_v1_body: withCacheOrForward(config, chainHead_v1_body),
		chainHead_v1_call: withCacheOrForward(config, chainHead_v1_call),
		chainHead_v1_continue: opCacheEnabled
			? ignoreRequest
			: chainHead_v1_forward,
		chainHead_v1_stopOperation: opCacheEnabled
			? ignoreRequest
			: chainHead_v1_forward,

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

function withCacheOrForward(
	config: CacheConfig,
	factory: (config?: MethodCacheConfig) => JSONRPCMethodHandler,
): JSONRPCMethodHandler {
	const methodName = factory.name;
	const enabled =
		config.enabled &&
		config.methods &&
		config.methods[methodName] &&
		config.methods[methodName].enabled;

	if (!enabled) return chainHead_v1_forward;

	return factory(config.methods[methodName]);
}
