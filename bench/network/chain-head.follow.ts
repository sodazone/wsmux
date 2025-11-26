import { printSummary, runChainHeadBench, type Script } from "./chain-head";

const PROVIDERS = [
	"ws://localhost:8181",
	"wss://polkadot.api.onfinality.io/public-ws",
];

export const basicScript = (): Script => ({
	onOpen: (send) => {
		send("follow", "chainHead_v1_follow", [true]);
	},

	onEvent: (subId, ev, send) => {
		if (ev.event === "newBlock") {
			send("header", "chainHead_v1_header", [subId, ev.blockHash]);
			send("body", "chainHead_v1_body", [subId, ev.blockHash]);
		}
	},
	onError: (_p, _s, error) => {
		console.error(`Error: ${error.message}`);
	},
});

(async () => {
	console.log("Starting benchmark…");

	const stats = runChainHeadBench(
		PROVIDERS[0]!,
		{ iterations: 20, warmup: 0 },
		basicScript,
	);

	setTimeout(() => {
		printSummary(stats);
		process.exit(0);
	}, 10_000);
})();
