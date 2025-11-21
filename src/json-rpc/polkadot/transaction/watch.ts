import { createDefaultSubscriptionHandler } from "../common";

export const transactionWatch_v1_submitAndWatch = () => {
	return createDefaultSubscriptionHandler(
		"transactionWatch_v1_watchEvent",
		new Set(["error", "invalid", "dropped", "finalized"]),
	);
};
