import type { MethodCacheConfig } from "@/config";
import type { JSONRPCMethodHandler } from "../../methods";
import type { JSONRPCRequest } from "../../types";
import { chainHead_v1_operation } from "./ops";

export const chainHead_v1_body = ({
	maxSize = 25,
}: MethodCacheConfig = {}): JSONRPCMethodHandler => {
	const keyOf: (req: JSONRPCRequest) => string = ({ params }) => params[1];

	return chainHead_v1_operation({
		maxSize,
		keyOf,
		terminalEvents: ["operationBodyDone"],
	});
};
