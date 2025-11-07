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

function normalizeUpstream(u: UpstreamConfig): UpstreamServerConfig {
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
	};
}

function normalizeConfig(raw: ProxyConfig): NormalizedConfig {
	return {
		logLevel: raw.log_level ?? "info",
		maxOpenSockets: raw.maxOpenSockets,
		jsonRpc: raw.json_rpc,
		upstream: raw.upstream.map(normalizeUpstream),
	};
}

let CONFIG: NormalizedConfig | null = null;

export function getConfig(): NormalizedConfig {
	if (!CONFIG) {
		throw new Error("Config not loaded yet. Call loadConfig() first.");
	}
	return CONFIG;
}

function setConfig(c: NormalizedConfig) {
	CONFIG = c;
}

export async function loadConfig(path = "./wsmux.config.yaml") {
	try {
		const text = await Bun.file(path).text();
		const raw = YAML.parse(text) as ProxyConfig;
		const normalized = normalizeConfig(raw);

		setConfig(normalized);
		return normalized;
	} catch (err) {
		throw new Error(`Unable to load config: "${path}"`, { cause: err });
	}
}
