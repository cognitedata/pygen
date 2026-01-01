# Overview over Tasks to Complete

These are tasks that are skipped from the first pass on a phase, but needs to be completed
before the v2.0 release. They are in no particular order, but have a number such
that they can be easily referenced.

## Deferred Tasks from Previous Phases

1. ~~**Add DataModelResponseWithViews**~~
   - When passing `inline_views` when retrieving a DataModelResponse, the server response
     with a DataModelResponseWithViews should contain a list of `ViewResponses` instead of 
     `ViewReferences`. 
2. **Implement InstanceAPI.upsert(mode="update")**
  - This currently raises NotImplementedError, but should be implemented to allow
    updating existing instances without affecting other fields. The design of how to do this
    is not yet finalized.
3. **Ensure client name is set correctly in PygenClient**
   - This is to ensure appropriate logging and tracking on the server side.

## Phase 4 Tasks (In Progress)

Scaffolding is complete. The following tasks remain:

4. **Task 4.1: Extend PygenModel for Connections**
   - Create `Connection` class in `_pygen_model/_connection.py`
   - Support direct relations, edge connections, reverse direct relations
   - Update DataClass and related models with connection support

5. **Task 4.2: Extend Transformer for Connections**
   - Handle `MultiEdgeProperty`, `SingleReverseDirectRelation`, `MultiReverseDirectRelation`
   - Add view reference resolution and self-referential handling
   - Handle inheritance (`implements`)

6. **Task 4.3: Add Validation Layer**
   - Create `_generator/validation.py` with pre-generation checks
   - Validate reverse relation targets exist, sources defined, reserved word conflicts
   - Generate clear warnings and error messages

7. **Task 4.4: Complete Python Data Class Templates**
   - Implement `create_import_statements()`, `generate_read_class()`, etc.
   - Located in `python.py` `PythonDataClassGenerator`

8. **Task 4.5: Complete Python API Class Templates**
   - Implement `create_api_class_code()` in `PythonGenerator`
   - Generate type-safe methods with unpacked filter parameters

9. **Task 4.6: Complete Python Client & Package Templates**
   - Client class template extending InstanceClient
   - Package structure (`__init__.py` files)
   - Implement `add_instance_api()` for standalone SDKs

10. **Task 4.7-4.8: Complete TypeScript Templates**
    - Data class, API class, and client templates for TypeScript
    - Handle TypeScript-specific idioms (readonly, interfaces)

11. **Task 4.9: Code Formatting Integration**
    - ruff format for Python, deno fmt for TypeScript
    - Handle formatting errors gracefully

12. **Task 4.10: Testing & Validation**
    - Unit tests for templates, integration tests comparing to example SDK
    - Edge case testing (self-referential, reserved words, large models)
