/**
 * Integration tests for authentication.
 *
 * These tests require environment variables to be set (either in .env or in the environment).
 * Required variables:
 *   - CDF_CLUSTER: The CDF cluster (e.g., "api", "westeurope-1")
 *   - CDF_PROJECT: The CDF project name
 *   - IDP_TENANT_ID: Azure AD tenant ID
 *   - IDP_CLIENT_ID: OAuth2 client ID
 *   - IDP_CLIENT_SECRET: OAuth2 client secret
 */

import { describe, it, expect, beforeAll } from "vitest";
import { execSync } from "child_process";
import { existsSync } from "fs";
import { resolve } from "path";
import { config as loadDotenv } from "dotenv";
import {
  HTTPClient,
  OAuthClientCredentials,
  isSuccess,
  type PygenClientConfig,
} from "@cognite/pygen-typescript";

/**
 * Try to find the repository root by running git command.
 */
function findRepoRoot(): string | null {
  try {
    const result = execSync("git rev-parse --show-toplevel", {
      encoding: "utf-8",
      stdio: ["pipe", "pipe", "pipe"],
    });
    return result.trim();
  } catch {
    return null;
  }
}

/**
 * Load environment variables from .env file if it exists.
 */
function loadEnvFile(): void {
  const repoRoot = findRepoRoot();
  if (repoRoot) {
    const dotenvPath = resolve(repoRoot, ".env");
    if (existsSync(dotenvPath)) {
      loadDotenv({ path: dotenvPath, override: true });
    }
  }
}

/**
 * Check if all required environment variables are set.
 */
function hasRequiredEnvVars(): boolean {
  const required = [
    "CDF_CLUSTER",
    "CDF_PROJECT",
    "IDP_TENANT_ID",
    "IDP_CLIENT_ID",
    "IDP_CLIENT_SECRET",
  ];
  return required.every((key) => process.env[key] !== undefined);
}

/**
 * Get a required environment variable or throw an error.
 */
function getEnvVar(name: string): string {
  const value = process.env[name];
  if (value === undefined) {
    throw new Error(`Required environment variable ${name} is not set`);
  }
  return value;
}

/**
 * Create a PygenClientConfig from environment variables.
 */
function createClientConfig(): PygenClientConfig {
  const cluster = getEnvVar("CDF_CLUSTER");
  const project = getEnvVar("CDF_PROJECT");
  const tenantId = getEnvVar("IDP_TENANT_ID");
  const clientId = getEnvVar("IDP_CLIENT_ID");
  const clientSecret = getEnvVar("IDP_CLIENT_SECRET");

  return {
    baseUrl: `https://${cluster}.cognitedata.com`,
    project,
    credentials: new OAuthClientCredentials({
      tokenUrl: `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`,
      clientId,
      clientSecret,
      scopes: [`https://${cluster}.cognitedata.com/.default`],
    }),
  };
}

describe("Authentication Integration", () => {
  beforeAll(() => {
    loadEnvFile();
  });

  it("should authenticate with OAuth2 credentials", async () => {
    if (!hasRequiredEnvVars()) {
      console.log("Skipping: Required environment variables not set");
      return;
    }

    const config = createClientConfig();
    const client = new HTTPClient(config);

    const result = await client.request({
      endpointUrl: `/api/v1/projects/${config.project}`,
      method: "GET",
    });

    expect(isSuccess(result)).toBe(true);
    if (isSuccess(result)) {
      expect(result.statusCode).toBe(200);
    }
  });

  it("should inspect token successfully", async () => {
    if (!hasRequiredEnvVars()) {
      console.log("Skipping: Required environment variables not set");
      return;
    }

    const config = createClientConfig();
    const client = new HTTPClient(config);

    const result = await client.request({
      endpointUrl: "/api/v1/token/inspect",
      method: "GET",
    });

    expect(isSuccess(result)).toBe(true);
    if (isSuccess(result)) {
      expect(result.statusCode).toBe(200);
      const body = JSON.parse(result.body) as { subject: string; projects: unknown[] };
      expect(body.subject).toBeDefined();
      expect(body.projects).toBeDefined();
    }
  });
});
