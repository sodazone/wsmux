import { configure, getConsoleSink, type LogLevel } from "@logtape/logtape";

export async function initLogger(lowestLevel?: LogLevel) {
	await configure({
		sinks: { console: getConsoleSink() },
		loggers: [
			{ category: "wsmux", lowestLevel, sinks: ["console"] },
			{
				category: ["logtape", "meta"],
				lowestLevel: "warning",
				sinks: ["console"],
			},
		],
	});
}
