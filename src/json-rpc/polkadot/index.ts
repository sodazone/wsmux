import type { JSONRPCMethodHandler } from "../methods";
import { chainHead_v1_follow } from "./chain-head/follow";
import {
	cachingForwardChainHeadHandler,
	forwardChainHeadHandler,
} from "./chain-head/forward";
import { chainHead_v1_unfollow } from "./chain-head/unfollow";
import { chainHead_v1_unpin } from "./chain-head/unpin";

export function polkadotMethods() {
	return {
		rpc_methods: {
			handleRequest: async (upstream, downstream, req) => {
				const response = await upstream.request(req);
				downstream.send(response);
			},
		},
		chainHead_v1_follow: chainHead_v1_follow(),
		chainHead_v1_unfollow: chainHead_v1_unfollow,
		chainHead_v1_unpin: chainHead_v1_unpin,
		chainHead_v1_header: cachingForwardChainHeadHandler(),
		chainHead_v1_storage: forwardChainHeadHandler,
		chainHead_v1_body: cachingForwardChainHeadHandler(),
		chainHead_v1_call: forwardChainHeadHandler,
		chainHead_v1_continue: forwardChainHeadHandler,
		chainHead_v1_stopOperation: forwardChainHeadHandler,
	} as const satisfies Record<string, JSONRPCMethodHandler>;
}
