import { configure, getConsoleSink } from "@logtape/logtape";

export async function initLogger() {
	await configure({
		sinks: { console: getConsoleSink() },
		loggers: [
			{
				category: "wsmux.chainhead.follow",
				lowestLevel: "debug",
				sinks: ["console"],
			},
			{ category: "wsmux", lowestLevel: "info", sinks: ["console"] },
			{
				category: ["logtape", "meta"],
				lowestLevel: "warning",
				sinks: ["console"],
			},
		],
	});
}
