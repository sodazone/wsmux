import { parseArgs } from "node:util";

import { VERSION } from "../version";

const DEFAULTS = {
	config: process.env.WSMUX_CONFIG ?? "./wsmux.config.yaml",
	listen: process.env.WSMUX_LISTEN || ":8181",
};

/**
 * Parses a TCP listen address string into hostname and port.
 * Supported formats: [::1]:9000, host:port, :port
 *
 * @param raw The raw listen address string.
 * @returns An object with hostname and port properties.
 */
function parseTcpListen(raw: string): { hostname: string; port: number } {
	if (!raw) throw new Error("Empty listen value");

	const ipv6 = raw.match(/^\[(.*)\]:(\d+)$/);
	if (ipv6) {
		return {
			hostname: ipv6[1] ?? "::",
			port: Number(ipv6[2]),
		};
	}

	const idx = raw.lastIndexOf(":");
	if (idx === -1) {
		throw new Error(
			`Invalid --listen value "${raw}" (expected host:port or :port)`,
		);
	}

	const hostname = raw.slice(0, idx) ?? "0.0.0.0";
	const port = Number(raw.slice(idx + 1));

	if (port < 1 || port > 65535) {
		throw new Error(`Invalid port in --listen "${raw}" (${port})`);
	}

	return { hostname, port };
}

export function getOpts() {
	const { values, positionals } = parseArgs({
		args: Bun.argv.slice(2),
		options: {
			help: { type: "boolean", short: "h" },
			version: { type: "boolean", short: "V" },
			config: { type: "string", short: "c" },
			listen: { type: "string", short: "l" },
		},
		strict: true,
		allowPositionals: true,
	});

	if (values.version) {
		console.log(`wsmux ${VERSION}`);
		process.exit(0);
	}

	if (values.help) {
		console.log(`
Usage: wsmux [options]

Options:
  -c, --config <path>   Configuration file
  -l, --listen <addr>   Listening address (host:port, :port, [::]:port)
  -V, --version         Show version number
  -h, --help            Show this help message

Environment variables:
  WSMUX_CONFIG          Path to config file
  WSMUX_LISTEN          Default listen address
`);
		process.exit(0);
	}

	return {
		config: values.config ?? DEFAULTS.config,
		listen: parseTcpListen(values.listen ? values.listen : DEFAULTS.listen),
		positionals,
	};
}
