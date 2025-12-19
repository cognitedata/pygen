# Rewrite of Pygen

This project aims to rewrite the Pygen package from scratch, the goal is to improve the 
performance, maintainability, and usability of the original package while preserving its core functionality.


## Problems

1. **Performance**: The original Pygen package wraps the `cognite-sdk` package for requests.
  This causes it not be able to take full advantage of the pydantic serialization and validation features.
  In addition, the `cognite-sdk` builds on `requests`, instead of `httpx`, which limits the performance and 
  scalability of the package.
2. **Performance**: Currently all queries in the generated SDK are eagerly evaluated. This means that the generated
  SDK will no scale as the amount of data increases. Instead, it should pass a client to each data objects, such
  that each data object can iterate over the data on demand.
3. **New features**: Currently Pygen only supports generating a Python SDK. However, we will be adding other SDK
  generated for other languages such as TypeScript, C#, and PySpark. The internal architecture needs an overhaul
  to support multiple languages.
4. **Maintainability**: The current codebase has low test coverage, just above 70%. Going forward, we want to achieve
  and above 90% test coverage to ensure the quality and reliability of the package.

## Goals
1. Create an internal Pygen Client to replace the `cognite-sdk`. It should support CRUD operations 
data models, views, containers, and spaces. These object types should be represented as pydantic models.
2. Implement lazy evaluation for by adding a Pygen Client to each data object that has a connection. The structure
   here is not fully decided yet, this will require some experimentation.
3. Rework the internal architecture to be more coherent and based on pydantic model. The current structer is 
  organically grown with the development of the original Pygen package, and needs a redesign to support multiple languages.
4. Maintain the current functionality of the original Pygen package, while improving performance and usability.
5. **New Feature**. Currently, Pygen can be served as a Python package or a CLI. However, we want to 
  support a Pygen backend service that can generate SDKs on demand via an API.
6. Data models can be incomplete, for example, that a reverse direct relation points to a direct
  relation that is not there. The new Pygen should do validation of the data model up front, give
  the appropriate warning, and gracefully generate as much of the SDK as possible. Today, Pygen
  does these types of checks ad-hoc during code generation, which leads to poor testability and
  has historically caused a lot of bugs.
