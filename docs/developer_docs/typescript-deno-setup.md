# TypeScript Development with Deno

## Overview

Pygen's TypeScript SDK is developed using **Deno** as the primary runtime. Deno provides several advantages over traditional Node.js development:

- **Native TypeScript support** - No transpilation needed for development
- **Modern ES modules** by default
- **Built-in tooling** - formatter, linter, test runner, bundler
- **Secure by default** - explicit permissions required
- **Better standard library** and web API compatibility
- **NPM compatibility** - can still use npm packages when needed

## Prerequisites

### Installing Deno

**Windows (PowerShell):**
```powershell
irm https://deno.land/install.ps1 | iex
```

**macOS/Linux:**
```bash
curl -fsSL https://deno.land/install.sh | sh
```

**Using package managers:**
- Homebrew: `brew install deno`
- Chocolatey: `choco install deno`
- Scoop: `scoop install deno`

Verify installation:
```bash
deno --version
```

## Development Workflow

### Project Structure

```
pygen/
├── cognite/pygen/_generation/typescript/  # TypeScript source code
│   └── instance_api/
│       ├── auth/                          # Authentication
│       ├── http_client/                   # HTTP client
│       └── types/                         # Type definitions
├── tests/typescript/                      # Tests
│   └── __tests__/
├── deno.json                              # Deno configuration
├── vitest.config.ts                       # Test configuration
└── tsconfig.json                          # TypeScript configuration
```

### Available Commands

All commands are defined in `deno.json` under the `tasks` section:

#### Type Checking
```bash
# Check TypeScript types
deno task typecheck
```

#### Testing
```bash
# Run all tests
deno task test

# Run tests in watch mode
deno task test:watch

# Run tests with coverage
deno task test:coverage

# Run integration tests
deno task test:integration
```

#### Code Formatting
```bash
# Format code
deno task format

# Check formatting without changes
deno task format:check
```

#### Linting
```bash
# Lint code
deno task lint

# Auto-fix linting issues
deno task lint:fix
```

### Deno Configuration

The `deno.json` file configures Deno's behavior:

```json
{
  "compilerOptions": {
    "strict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noImplicitReturns": true,
    "noFallthroughCasesInSwitch": true,
    "noUncheckedIndexedAccess": true,
    "lib": ["ES2022", "DOM"]
  },
  "imports": {
    "@cognite/pygen-typescript": "./cognite/pygen/_typescript/instance_api/index.ts"
  },
  "fmt": {
    "useTabs": false,
    "lineWidth": 100,
    "indentWidth": 2,
    "semiColons": true,
    "singleQuote": false
  }
}
```

### Import Maps

Deno uses import maps for module resolution. The main SDK can be imported using the alias:

```typescript
import { HTTPClient, TokenCredentials } from "@cognite/pygen-_typescript";
```

This alias is configured in `deno.json` under `imports`.

## Writing Code

### Import Statements

**Always use explicit `.ts` file extensions** in import statements:

```typescript
// ✅ Good
import type { Credentials } from "./credentials.ts";
import { HTTPClient } from "./client.ts";

// ❌ Bad - will not work with Deno
import type { Credentials } from "./credentials";
import { HTTPClient } from "./client.js";
```

### Standard Web APIs

Prefer standard Web APIs over Node.js-specific APIs:

```typescript
// ✅ Good - standard fetch API
const response = await fetch(url, { method: "POST", body: JSON.stringify(data) });

// ✅ Good - standard FormData
const formData = new FormData();
formData.append("key", "value");

// ❌ Bad - Node.js specific
const http = require("http");
```

### TypeScript Configuration

TypeScript is configured for strict mode with additional safety checks:

- `strict: true` - Enable all strict type checking
- `noUnusedLocals: true` - Error on unused local variables
- `noUnusedParameters: true` - Error on unused function parameters
- `noImplicitReturns: true` - Error on missing return statements
- `noUncheckedIndexedAccess: true` - Require checks for indexed access

## Testing

### Test Framework

Tests use **Vitest** (running through Deno) for maximum compatibility:

```typescript
import { describe, it, expect } from "vitest";

describe("MyFeature", () => {
  it("should work correctly", () => {
    const result = myFunction();
    expect(result).toBe(expectedValue);
  });
});
```

### Running Tests

```bash
# Run all unit tests
deno task test

# Run with coverage
deno task test:coverage

# Run integration tests
deno task test:integration

# Watch mode for development
deno task test:watch
```

### Test Files

- Unit tests: `tests/typescript/**/*.test.ts`
- Integration tests: `tests/typescript/**/*.integration.test.ts`
- Test utilities: `tests/typescript/utils/`

## CI/CD Integration

The GitHub Actions workflow uses Deno for all TypeScript operations:

```yaml
- name: Setup Deno
  uses: denoland/setup-deno@v2
  with:
    deno-version: v2.x

- name: TypeScript compilation check
  run: deno task typecheck

- name: Run tests with coverage
  run: deno task test:coverage
```

## Compatibility

### Generated SDK Compatibility

The generated TypeScript SDK is designed to work in multiple environments:

- **Deno** - Native support
- **Node.js** - Works with Node 18+ and ESM
- **Browsers** - Compatible with modern browsers
- **Bundlers** - Works with Webpack, Vite, esbuild, etc.

### NPM Package Compatibility

Deno has excellent npm compatibility. If you need to use npm packages:

```typescript
// Import npm packages with npm: specifier
import { somePackage } from "npm:package-name@version";
```

The `deno.json` is configured with `"nodeModulesDir": "auto"` to automatically create a `node_modules` directory when needed.

## Troubleshooting

### Permission Errors

Deno is secure by default. If you encounter permission errors, you may need to grant explicit permissions:

```bash
# Allow network access
deno run --allow-net script.ts

# Allow environment variables
deno run --allow-env script.ts

# Allow all permissions (development only)
deno run -A script.ts
```

### Module Resolution Issues

If imports aren't resolving correctly:

1. Check that import statements use `.ts` extensions
2. Verify the import map in `deno.json`
3. Run `deno cache` to refresh cached modules

### Type Checking Failures

Run type checking to see detailed errors:

```bash
deno task typecheck
```

Common issues:
- Missing type annotations
- Incorrect type assertions
- Unused variables or parameters

## Best Practices

### 1. Use Explicit Types

```typescript
// ✅ Good
function processData(data: string): number {
  return data.length;
}

// ❌ Bad
function processData(data) {
  return data.length;
}
```

### 2. Prefer Immutability

```typescript
// ✅ Good
const config = { readonly: true } as const;

// ❌ Bad
let config = { readonly: true };
config = { readonly: false };
```

### 3. Use Type Guards

```typescript
// ✅ Good
function isString(value: unknown): value is string {
  return typeof value === "string";
}

if (isString(data)) {
  // TypeScript knows data is string here
  console.log(data.toUpperCase());
}
```

### 4. Leverage Union Types

```typescript
// ✅ Good
type Result = { success: true; data: string } | { success: false; error: string };

// ❌ Bad
type Result = { success: boolean; data?: string; error?: string };
```

## Resources

- [Deno Manual](https://deno.land/manual)
- [Deno Standard Library](https://deno.land/std)
- [Deno Deploy](https://deno.com/deploy)
- [TypeScript Handbook](https://www.typescriptlang.org/docs/)
- [Vitest Documentation](https://vitest.dev/)

## Migration Notes

### From Node.js to Deno

If migrating existing Node.js code:

1. Update imports to use `.ts` extensions
2. Replace Node-specific APIs with Web standards
3. Remove `package.json` dependencies where possible
4. Update import statements to use Deno-style imports
5. Test with `deno task test`

### Backward Compatibility

The generated SDK maintains backward compatibility with Node.js environments by:

- Using standard Web APIs (fetch, FormData, etc.)
- Avoiding Deno-specific APIs in generated code
- Supporting both ESM and CommonJS module systems
- Providing TypeScript type definitions

