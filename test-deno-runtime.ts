#!/usr/bin/env -S deno run --allow-read --allow-env

/**
 * Test script to verify Deno runtime compatibility.
 * 
 * This script validates that:
 * 1. TypeScript source files can be imported and type-checked by Deno
 * 2. Core functionality works correctly
 * 3. No runtime errors occur
 */

import {
  createInstanceId,
  createViewReference,
  instanceIdEquals,
  instanceIdToString,
  viewReferenceToString,
  msToDate,
  dateToMs,
  TokenCredentials,
  type InstanceId,
  type ViewReference,
} from "./cognite/pygen/_generation/typescript/instance_api/index.ts";

console.log("🧪 Testing Deno Runtime Compatibility...\n");

let testsPassed = 0;
let testsFailed = 0;

function test(name: string, fn: () => void) {
  try {
    fn();
    testsPassed++;
    console.log(`✅ ${name}`);
  } catch (error) {
    testsFailed++;
    console.error(`❌ ${name}`);
    console.error(`   Error: ${error}`);
  }
}

// Test 1: Reference types
test("createInstanceId creates correct instance ID", () => {
  const id = createInstanceId("mySpace", "myInstance");
  if (id.space !== "mySpace" || id.externalId !== "myInstance") {
    throw new Error("Instance ID properties don't match");
  }
});

test("createViewReference creates correct view reference", () => {
  const ref = createViewReference("mySpace", "myView", "v1");
  if (ref.space !== "mySpace" || ref.externalId !== "myView" || ref.version !== "v1") {
    throw new Error("View reference properties don't match");
  }
});

test("instanceIdEquals compares instance IDs correctly", () => {
  const id1 = createInstanceId("space1", "id1");
  const id2 = createInstanceId("space1", "id1");
  const id3 = createInstanceId("space2", "id2");
  
  if (!instanceIdEquals(id1, id2)) {
    throw new Error("Equal instance IDs not recognized as equal");
  }
  if (instanceIdEquals(id1, id3)) {
    throw new Error("Different instance IDs recognized as equal");
  }
});

test("instanceIdToString formats correctly", () => {
  const id = createInstanceId("mySpace", "myInstance");
  const str = instanceIdToString(id);
  if (str !== "mySpace:myInstance") {
    throw new Error(`Expected 'mySpace:myInstance', got '${str}'`);
  }
});

test("viewReferenceToString formats correctly", () => {
  const ref = createViewReference("mySpace", "myView", "v1");
  const str = viewReferenceToString(ref);
  if (str !== "mySpace:myView:v1") {
    throw new Error(`Expected 'mySpace:myView:v1', got '${str}'`);
  }
});

// Test 2: Date utilities
test("msToDate converts milliseconds to Date", () => {
  const ms = 1703721600000; // 2023-12-28T00:00:00.000Z
  const date = msToDate(ms);
  if (date.getTime() !== ms) {
    throw new Error("Date conversion failed");
  }
  if (date.toISOString() !== "2023-12-28T00:00:00.000Z") {
    throw new Error("Date ISO string incorrect");
  }
});

test("dateToMs converts Date to milliseconds", () => {
  const date = new Date("2023-12-28T00:00:00.000Z");
  const ms = dateToMs(date);
  if (ms !== 1703721600000) {
    throw new Error(`Expected 1703721600000, got ${ms}`);
  }
});

test("msToDate and dateToMs are inverse operations", () => {
  const originalMs = 1703721600000;
  const date = msToDate(originalMs);
  const convertedMs = dateToMs(date);
  if (originalMs !== convertedMs) {
    throw new Error("Round trip conversion failed");
  }
});

// Test 3: Authentication
test("TokenCredentials creates proper auth header", async () => {
  const creds = new TokenCredentials("test-token-123");
  const [headerName, headerValue] = await creds.authorizationHeader();
  
  if (headerName !== "Authorization") {
    throw new Error(`Expected header name 'Authorization', got '${headerName}'`);
  }
  if (headerValue !== "Bearer test-token-123") {
    throw new Error(`Expected 'Bearer test-token-123', got '${headerValue}'`);
  }
});

// Test 4: Type checking
test("TypeScript types are correctly inferred", () => {
  const id: InstanceId = createInstanceId("space", "id");
  const ref: ViewReference = createViewReference("space", "view", "v1");
  
  // If this compiles, types are working correctly
  const _space: string = id.space;
  const _externalId: string = id.externalId;
  const _version: string = ref.version;
  
  // Unused variables are expected here - we're just testing types
  void _space;
  void _externalId;
  void _version;
});

// Summary
console.log("\n" + "=".repeat(50));
console.log(`Tests passed: ${testsPassed}`);
console.log(`Tests failed: ${testsFailed}`);
console.log("=".repeat(50));

if (testsFailed > 0) {
  console.error("\n❌ Some tests failed!");
  Deno.exit(1);
} else {
  console.log("\n✅ All tests passed! Deno runtime is compatible.");
  Deno.exit(0);
}

