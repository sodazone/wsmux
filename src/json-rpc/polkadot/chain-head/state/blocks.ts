import { getLogger } from "@logtape/logtape";

import type { JSONRPCNotification } from "../../../types";

const logger = getLogger(["wsmux", "chainhead", "state.blocks"]);

export function createBlockTracker(
	maxKnown = 512,
	onApply: (msg: JSONRPCNotification) => void,
) {
	const known = new Set<string>();
	const pending: JSONRPCNotification[] = [];

	function remember(hash?: string) {
		if (!hash) {
			return;
		}

		known.add(hash);
		if (known.size > maxKnown) {
			let excess = known.size - maxKnown;
			for (const h of known) {
				known.delete(h);
				if (--excess <= 0) break;
			}
		}
	}

	function flushPending() {
		let applied = true;
		while (applied) {
			applied = false;
			for (let i = 0; i < pending.length; ) {
				const msg = pending[i];
				if (!msg) {
					i++;
					continue;
				}
				const parent = msg?.params?.result?.parentBlockHash;
				if (!parent || known.has(parent)) {
					remember(msg.params?.result?.blockHash);

					pending.splice(i, 1);
					onApply(msg);
					applied = true;
				} else {
					i++;
				}
			}
		}
	}

	function handleNewBlock(msg: JSONRPCNotification) {
		const event = msg.params?.result;
		const parent = event?.parentBlockHash;

		if (parent && !known.has(parent)) {
			const blockHash = event?.blockHash;
			if (pending.some((p) => p.params?.result?.blockHash === blockHash)) {
				logger.debug((l) => l`Duplicate pending block ${blockHash}, skipping`);
				return false;
			}

			pending.push(msg);
			logger.debug(
				(l) =>
					l`Queued block ${blockHash} (unknown parent ${parent}), pending=${pending.length}`,
			);
			return false;
		}

		remember(event?.blockHash);
		flushPending();
		return true;
	}

	return {
		remember,
		handleNewBlock,
		flushPending,
		known,
		get stats() {
			return { known: known.size, pending: pending.length };
		},
	};
}
