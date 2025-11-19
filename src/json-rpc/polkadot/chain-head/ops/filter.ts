import { filter, type Observable } from "rxjs";
import type { JSONRPCNotification } from "@/json-rpc/types";

const RESULTS = ["limitReached", "started"];

export function filterOperationEvents(src$: Observable<JSONRPCNotification>) {
	return src$.pipe(
		filter((o) => {
			const isOperationEvent =
				o.params?.result &&
				(RESULTS.includes(o.params.result) ||
					o.params.result?.operationId !== undefined);
			return !isOperationEvent;
		}),
	);
}
