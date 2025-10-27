import type { JSONRPCNotification, JSONRPCRequest } from "../../types";

export function followNotifyTransform(request: JSONRPCRequest) {
	if (request.params?.[0] === true) {
		return;
	}

	return (notification: JSONRPCNotification): JSONRPCNotification => {
		if (notification.method !== "chainHead_v1_followEvent") {
			return notification;
		}

		const result = notification.params?.result;
		if (!result?.event) {
			return notification;
		}

		switch (result.event) {
			case "initialized": {
				if ("finalizedBlockRuntime" in result) {
					const newResult = { ...result };
					delete (newResult as Record<string, unknown>).finalizedBlockRuntime;
					return {
						...notification,
						params: { ...notification.params, result: newResult },
					};
				}
				break;
			}

			case "newBlock": {
				if ("newRuntime" in result) {
					const newResult = { ...result };
					delete (newResult as Record<string, unknown>).newRuntime;
					return {
						...notification,
						params: { ...notification.params, result: newResult },
					};
				}
				break;
			}
		}

		return notification;
	};
}
