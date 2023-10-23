# osdu pygen example

Motivation is to extend pygen usage

- to load JSON from an external system into Cognite Data Model
  - where the data-model is a "source-model" mirroring 1:1 the JSON sturcture (schema)
- that for minimal extension to pygen were required
  - `DomainModelApply` base-class:
    - making `external_id` optional and connected to a `model_validator` decorator, which allows to add a custom **externalId-factory**
      - done with injecting `monkey-patches/osdu/data_classes/_core_path.py`
      - into `_core.py`
    - extending pygen to create a SDK with alias-support in `DomainModelApply` classes too (prior only for `DomainModel` classes) (
      - done with `monkey-patches/pygen/_core/data_classes.py`

## create pygen sdk for osdu-example schema

- the osdu example model
  - `examples-osdu/osdu-schema/osdu-example.graphql`
  - created with [json-to-graphql](https://transform.tools/json-to-graphql) and composing a model which covers the three example-json documents
    - WellApply
    - WellboreApply
    - WellboreTrajectoryApply
  - source: <https://community.opengroup.org/osdu/data/data-definitions/-/tree/master/Examples/master-data?ref_type=heads>
  - one simplification made from the original JSON examples
    - due to missing data and schema
    - was removing occurrences of
      - `properties: {}`
      - `ExternalProperties: {}`
    - files marked with `.patched.json` suffix

- create pygen sdk for osdu-example schema
  - requires manual patching of `_core.py` > `DomainModelApply`
  - see `./monkey-patches/osdu/data_classes/_core.py`

```bash
pygen generate --space osdu-pygen-spc --external-id osdu_pygen_example --version 1 --output-dir ./examples-osdu/src/osdu --overwrite --client-name osdu --tenant-id
    <your tenantId > --client-id <client-id> --client-secret ***redacted*** --cdf-project <your-cdf-project> --cdf-cluster <cdf-cluster>
```

## patch and configure the externalId-factory

Patching the `osdu/data-classes/_core.py` was done manual by importing and injecting into `DomainApply` class, in this step the `extra` parameter was removed, as it is covered now in `DomainModelApplyPatch.model_config`.

Integration is done atm in `./tests/test_osdu_json_load.py`

- which uses `examples-osdu/tests/setup.py` to configure the externalId-factory

## version for externalId-factory

Pragmatic approach for rule-based and generic external-id generation are available in `./examples-osdu/monkey-patches/osdu/data_classes/extid_factory.py`
