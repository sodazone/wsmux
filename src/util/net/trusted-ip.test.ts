import { describe, expect, it } from "bun:test";
import { createTrustedIPs } from "./trusted-ip";

describe("Trusted IP matcher", () => {
	const trusted = createTrustedIPs([
		"10.0.0.0/8",
		"192.168.1.0/24",
		"172.16.0.0/12",
		"fd12:3456:789a::/48",
		"2001:db8::/32",
		"127.0.0.1/32",
	]);

	it("should trust IPv4 addresses inside the ranges", () => {
		expect(trusted.isTrusted("10.0.0.1")).toBe(true);
		expect(trusted.isTrusted("192.168.1.42")).toBe(true);
		expect(trusted.isTrusted("172.16.5.1")).toBe(true);
	});

	it("should not trust IPv4 addresses outside the ranges", () => {
		expect(trusted.isTrusted("8.8.8.8")).toBe(false);
		expect(trusted.isTrusted("192.168.2.1")).toBe(false);
	});

	it("should trust IPv6 addresses inside the ranges", () => {
		expect(trusted.isTrusted("fd12:3456:789a::1")).toBe(true);
		expect(trusted.isTrusted("2001:db8::1234")).toBe(true);
	});

	it("should not trust IPv6 addresses outside the ranges", () => {
		expect(trusted.isTrusted("fd13:3456:789a::1")).toBe(false);
		expect(trusted.isTrusted("2001:db9::1")).toBe(false);
	});

	it("should not trust empty string", () => {
		expect(trusted.isTrusted("")).toBe(false);
	});

	it("should trust IPv4 localhost", () => {
		expect(trusted.isTrusted("127.0.0.1")).toBe(true);
	});

	it("should reject everything if empty set", () => {
		const emptyTrusted = createTrustedIPs([]);
		expect(emptyTrusted.isTrusted("192.168.1.42")).toBe(false);
		expect(emptyTrusted.isTrusted("fd12:3456:789a::1")).toBe(false);
	});

	it("should reject malformed IPs", () => {
		expect(trusted.isTrusted("999.999.999.999")).toBe(false);
		expect(trusted.isTrusted("gggg::1")).toBe(false);
	});
});
