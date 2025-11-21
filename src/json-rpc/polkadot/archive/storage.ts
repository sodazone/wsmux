import { createDefaultSubscriptionHandler } from "../common";

export const archive_v1_storage = () => {
	return createDefaultSubscriptionHandler(
		"archive_v1_storageEvent",
		new Set(["storageDone"]),
	);
};

export const archive_v1_storageDiff = () => {
	return createDefaultSubscriptionHandler(
		"archive_v1_storageDiffEvent",
		new Set(["storageDiffDone"]),
	);
};
