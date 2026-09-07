// Description: This script is used to test the Pygen installation in a Pyodide environment.
// We are using JupyterLite and stlite, both pyodide based Python runtimes. To ensure that pygen works
// in these runtimes, we need to test the installation of the pygen in a Pyodide environment.
// This script will start an HTTP server to serve the pygen wheel file and then try to install the pygen in Python.
// If the installation is successful, it will run a simple Python script to test pygen.
const { loadPyodide } = require("pyodide");

const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = 3000;
const pyodideIndexURL = path.dirname(require.resolve("pyodide"));

// The Cognite Python SDK wheel filename will be sent in as environment variable
const wheelFilePath = path.join(__dirname, 'dist', process.env.PYGEN_FILE_PATH);

// Create an HTTP server to serve the wheel file
const server = http.createServer((req, res) => {
  fs.readFile(wheelFilePath, (err, data) => {
    if (err) {
      // Handle file read errors
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('500 - Internal Server Error');
    } else {
      // Serve the file content
      res.writeHead(200, { 'Content-Type': 'application/octet-stream' });
      res.end(data);
    }
  });
});

// Start the server and listen on the defined port. Then try to install the SDK in Python
server.listen(PORT, () => {
  console.log(`Server is running at http://localhost:${PORT}. Now trying to install sdk.`);

  async function test_cognite_sdk() {
    let pyodide =  await loadPyodide({ indexURL: pyodideIndexURL });
    await pyodide.loadPackage(["micropip"]);
    // In pyodide <0.28 the `ssl` stdlib module is unvendored and shipped as a
    // separate package that must be loaded explicitly. In newer pyodide
    // versions it is built in and no longer registered as a loadable package.
    try {
      await pyodide.loadPackage(["ssl"]);
    } catch (e) {
      // `ssl` not in this pyodide's package index; assume it is built in.
    }
    const micropip = pyodide.pyimport("micropip");

    // `cognite-sdk` transitively requires `cryptography>=45.0.1`. `cryptography`
    // is a compiled Rust extension: pyodide 0.27.6 (stlite) only ships a wasm
    // build for `cryptography==42.0.5`, and no pure-Python wheel exists on
    // PyPI. Register a mock distribution at the required version so micropip's
    // resolver treats the requirement as satisfied. Runtime code paths that
    // actually need `cryptography` (e.g. certain auth flows) are not exercised
    // by this smoke test.
    await pyodide.runPythonAsync(`
import micropip
micropip.add_mock_package("cryptography", "45.0.1")
`);

    // Read packages to install from environment variable as JSON
    const packages = JSON.parse(process.env.PACKAGES);
    await micropip.install(packages);
    await pyodide.runPythonAsync("from cognite.pygen import generate_sdk");

    return pyodide.runPythonAsync('"Pygen successfully installed and imported!"');
  }

  test_cognite_sdk().then((result) => {
    console.log("Response from Python =", result);
    server.close();
  }).catch((err) => {
    console.error("Test failed:", err);
    server.close();
    process.exitCode = 1;
  });
});
