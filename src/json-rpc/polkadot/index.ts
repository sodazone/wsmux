import type { JSONRPCMethodHandler } from "../methods";
import {
	chainHead_v1_follow,
	chainHead_v1_unfollow,
} from "./chain-head/follow";
import { forwardChainHeadHandler } from "./chain-head/forward";

export const polkadotMethods = {
	rpc_methods: {
		handleRequest: async (upstream, downstream, req) => {
			const response = await upstream.request(req);
			downstream.send(response);
		},
	},
	chainHead_v1_follow,
	chainHead_v1_unfollow,
	chainHead_v1_header: forwardChainHeadHandler,
	chainHead_v1_storage: forwardChainHeadHandler,
	chainHead_v1_body: forwardChainHeadHandler,
	chainHead_v1_call: forwardChainHeadHandler,
	chainHead_v1_continue: forwardChainHeadHandler,
	chainHead_v1_stopOperation: forwardChainHeadHandler,
	// TODO unpin (local version)
} as const satisfies Record<string, JSONRPCMethodHandler>;
