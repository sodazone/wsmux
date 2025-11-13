import type { JSONRPCMethodHandler } from "../methods";
import { chainHead_v1_follow } from "./chain-head/follow";
import {
	chainHead_v1_forward,
	chainHead_v1_header,
} from "./chain-head/forward";
import { chainHead_v1_unfollow } from "./chain-head/unfollow";
import { chainHead_v1_unpin } from "./chain-head/unpin";
import { forwardRequest } from "./forward";

export function polkadotMethods() {
	return {
		__fallback: forwardRequest,
		chainHead_v1_follow: chainHead_v1_follow(),
		chainHead_v1_unfollow: chainHead_v1_unfollow,
		chainHead_v1_unpin: chainHead_v1_unpin,
		chainHead_v1_header: chainHead_v1_header(),
		chainHead_v1_storage: chainHead_v1_forward,
		chainHead_v1_body: chainHead_v1_forward,
		chainHead_v1_call: chainHead_v1_forward,
		chainHead_v1_continue: chainHead_v1_forward,
		chainHead_v1_stopOperation: chainHead_v1_forward,
	} as const satisfies Record<string, JSONRPCMethodHandler>;
}
