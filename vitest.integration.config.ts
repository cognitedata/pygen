import { defineConfig } from "vitest/config";
import { resolve } from "path";

export default defineConfig({
  resolve: {
    alias: {
      "@cognite/pygen-typescript": resolve(
        __dirname,
        "cognite/pygen/_generation/typescript/instance_api/index.ts"
      ),
    },
  },
  test: {
    globals: true,
    environment: "node",
    include: ["tests/typescript/**/*.integration.test.ts"],
    testTimeout: 30000, // 30 seconds for integration tests
  },
});

