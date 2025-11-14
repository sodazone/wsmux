import { YAML } from "bun";
import { resolvePreset } from "../json-rpc/presets";
import type { UpstreamServerConfig } from "../json-rpc/upstream";
import type {
	DurationString,
	NormalizedConfig,
	ProxyConfig,
	RawUpstreamServerConfig,
} from "./types";

function parseDuration(
	s: DurationString | null | undefined,
	defaultMs?: number,
): number | undefined {
	if (!s) return defaultMs;

	if (s.endsWith("ms")) return Number(s.slice(0, -2));
	if (s.endsWith("s")) return Number(s.slice(0, -1)) * 1000;
	if (s.endsWith("m")) return Number(s.slice(0, -1)) * 60_000;

	throw new Error(`Invalid duration string: ${s}`);
}

function parseDurationWithDefault(
	s: DurationString | null | undefined,
	defaultMs: number,
): number {
	return parseDuration(s, defaultMs) ?? defaultMs;
}

function normalizeUpstreamServer(
	u: RawUpstreamServerConfig,
): UpstreamServerConfig {
	let supportedMethods: Set<string> | undefined;

	if (u.presets) {
		const presets = Array.isArray(u.presets) ? u.presets : [u.presets];
		supportedMethods = new Set(presets.flatMap(resolvePreset));
	} else {
		supportedMethods =
			u.supported_methods === "*" || !u.supported_methods
				? undefined
				: new Set(u.supported_methods);
	}
	return {
		name: u.name,
		url: u.url,
		requestTimeout: parseDuration(u.request_timeout),
		connectionTimeout: parseDuration(u.connection_timeout),
		retryDelay: parseDuration(u.retry_delay),
		maxConnections: u.max_connections,
		supportedMethods,
		methods: u.methods ?? {},
	};
}

function normalizeConfig(raw: ProxyConfig): NormalizedConfig {
	return {
		logLevel: raw.log_level ?? "info",
		rateLimit: {
			enabled: raw.rate_limit?.enabled ?? false,
			maxRequests: raw.rate_limit?.max_requests ?? 50,
			windowMs: parseDurationWithDefault(raw.rate_limit?.window, 60_000),
			trustedNetworks: raw.rate_limit?.trusted_networks ?? [],
		},
		jsonRpc: raw.json_rpc,
		upstream: {
			stateless: new Set(raw.upstream.stateless ?? []),
			debug: {
				stats: {
					enabled: raw.upstream.debug?.stats?.enabled ?? false,
					interval: parseDurationWithDefault(
						raw.upstream.debug?.stats?.interval,
						30_000,
					),
				},
			},
			servers: raw.upstream.servers.map(normalizeUpstreamServer),
		},
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
