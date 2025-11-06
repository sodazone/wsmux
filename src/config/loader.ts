import { YAML } from "bun";

import type { ProxyConfig } from "./types";

export async function loadConfig(
  path = "./wsmux.config.yaml",
): Promise<ProxyConfig> {
  try {
    const text = await Bun.file(path).text();
    return YAML.parse(text) as ProxyConfig;
  } catch (err) {
    throw new Error(`Unable to load config: "${path}"`, { cause: err });
  }
}
