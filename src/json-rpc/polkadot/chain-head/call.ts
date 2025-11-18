import type { MethodCacheConfig } from "@/config";
import type { JSONRPCMethodHandler } from "../../methods";
import type { JSONRPCRequest } from "../../types";
import { chainHead_v1_operation } from "./ops";

export const chainHead_v1_call = ({
	maxSize = 25,
}: MethodCacheConfig = {}): JSONRPCMethodHandler => {
	const keyOf: (req: JSONRPCRequest) => string = ({ params }) =>
		JSON.stringify([params[1], params[2], params[3]]);

	return chainHead_v1_operation({
		maxSize,
		keyOf,
		terminalEvents: ["operationCallDone"],
	});
};
