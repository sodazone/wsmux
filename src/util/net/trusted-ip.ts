import { isIP } from "node:net";

function ipv4ToLong(ip: string): number {
	return ip.split(".").reduce((acc, oct) => (acc << 8) + Number(oct), 0) >>> 0;
}

function parseIPv4CIDR(cidr: string): [number, number] {
	const [net, bitsStr] = cidr.split("/");
	const bits = bitsStr ? parseInt(bitsStr, 10) : 32;
	const mask = bits === 0 ? 0 : (0xffffffff << (32 - bits)) >>> 0;
	const base = ipv4ToLong(net!) & mask;
	return [base, mask];
}

function ipv6ToBigInt(ip: string): bigint {
	// expand "::" shorthand
	const full = ip.includes("::") ? expandIPv6(ip) : ip;
	const parts = full.split(":").map((p) => BigInt(parseInt(p, 16)));
	return parts.reduce((acc, part) => (acc << 16n) + part, 0n);
}

function expandIPv6(ip: string): string {
	const [left, right] = ip.split("::");
	const leftParts = left ? left.split(":") : [];
	const rightParts = right ? right.split(":") : [];
	const fill = Array(8 - leftParts.length - rightParts.length).fill("0");
	return [...leftParts, ...fill, ...rightParts].join(":");
}

function parseIPv6CIDR(cidr: string): [bigint, bigint] {
	const [net, bitsStr] = cidr.split("/");
	const bits = bitsStr ? parseInt(bitsStr, 10) : 128;
	const mask =
		bits === 0 ? 0n : ((1n << BigInt(128 - bits)) - 1n) ^ ((1n << 128n) - 1n);
	const base = ipv6ToBigInt(net!) & mask;
	return [base, mask];
}

export function createTrustedIPs(cidrList: string[]) {
	const ipv4Ranges: [number, number][] = [];
	const ipv6Ranges: [bigint, bigint][] = [];

	for (const cidr of cidrList) {
		if (cidr.includes(".")) ipv4Ranges.push(parseIPv4CIDR(cidr));
		else if (cidr.includes(":")) ipv6Ranges.push(parseIPv6CIDR(cidr));
	}

	return {
		isTrusted(addr: string) {
			if (ipv4Ranges.length === 0 && ipv6Ranges.length === 0) {
				return false;
			}

			const version = isIP(addr);
			if (version === 4 && ipv4Ranges.length > 0) {
				const ipNum = ipv4ToLong(addr);
				for (const [base, mask] of ipv4Ranges) {
					if ((ipNum & mask) === base) return true;
				}
			} else if (version === 6 && ipv6Ranges.length > 0) {
				const ipNum = ipv6ToBigInt(addr);
				for (const [base, mask] of ipv6Ranges) {
					if ((ipNum & mask) === base) return true;
				}
			}

			return false;
		},
	};
}
