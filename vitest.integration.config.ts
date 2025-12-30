import { defineConfig } from "vitest/config";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { dirname } from "node:path";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export default defineConfig({
  resolve: {
    alias: {
      "@cognite/pygen-typescript": resolve(
        __dirname,
        "cognite/pygen/_generation/_typescript/instance_api/index.ts"
      ),
    },
  },
  test: {
    globals: true,
    environment: "node",
    include: ["tests/_typescript/**/*.integration.test.ts"],
    testTimeout: 30000, // 30 seconds for integration tests
  },
});

