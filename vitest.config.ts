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
        "cognite/pygen/_generation/typescript/instance_api/index.ts"
      ),
    },
  },
  test: {
    globals: true,
    environment: "node",
    include: ["tests/typescript/**/*.test.ts"],
    exclude: ["tests/typescript/**/*.integration.test.ts", "node_modules"],
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "html"],
      include: ["cognite/pygen/_generation/typescript/**/*.ts"],
      exclude: ["node_modules", "tests"],
      thresholds: {
        lines: 90,
        functions: 90,
        branches: 90,
        statements: 90,
      },
    },
  },
});

