import { YAML } from "bun";
import type { UpstreamServerConfig } from "../json-rpc/upstream";
import type {
	DurationString,
	NormalizedConfig,
	ProxyConfig,
	UpstreamConfig,
} from "./types";

function parseDuration(s?: DurationString): number | undefined {
	if (!s) return undefined;

	if (s.endsWith("ms")) return Number(s.slice(0, -2));
	if (s.endsWith("s")) return Number(s.slice(0, -1)) * 1000;
	if (s.endsWith("m")) return Number(s.slice(0, -1)) * 60_000;

	throw new Error(`Invalid duration string: ${s}`);
}

function normalizeUpstreamWithDebug(debug: {
	stats: { enabled: boolean; interval: number };
}) {
	return (u: UpstreamConfig): UpstreamServerConfig => {
		return {
			name: u.name,
			url: u.url,

			requestTimeout: parseDuration(u.request_timeout),
			connectionTimeout: parseDuration(u.connection_timeout),
			retryDelay: parseDuration(u.retry_delay),
			maxConnections: u.max_connections,

			supportedMethods:
				u.supported_methods === "*" || !u.supported_methods
					? undefined
					: [...u.supported_methods],

			methods: u.methods ?? {},
			stats: debug.stats,
		};
	};
}

function normalizeConfig(raw: ProxyConfig): NormalizedConfig {
	const debug = {
		stats: {
			enabled: raw.debug?.stats?.enabled ?? false,
			interval: raw.debug?.stats?.interval
				? (parseDuration(raw.debug?.stats.interval) ?? 20_000)
				: 20_000,
		},
	};
	return {
		logLevel: raw.log_level ?? "info",
		debug,
		maxOpenSockets: raw.maxOpenSockets,
		jsonRpc: raw.json_rpc,
		upstream: raw.upstream.map(normalizeUpstreamWithDebug(debug)),
	};
}

let CONFIG: NormalizedConfig | null = null;

export function getConfig(): NormalizedConfig {
	if (!CONFIG) {
		throw new Error("Config not loaded yet. Call loadConfig() first.");
	}
	return CONFIG;
}

export async function loadConfig(path = "./wsmux.config.yaml") {
	try {
		// TODO: user friendly message and usage when config is not found or invalid
		const text = await Bun.file(path).text();
		const raw = YAML.parse(text) as ProxyConfig;
		CONFIG = normalizeConfig(raw);
		return CONFIG;
	} catch (err) {
		throw new Error(`Unable to load config: "${path}"`, { cause: err });
	}
}
