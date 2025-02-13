# Building a Streamlit App in CDF

**Prerequisites**
- Access to a CDF Project.
- Some familiarity with Streamlit.

## Settings in Streamlit App

Streamlit is a beta feature in CDF. You can use `pygen` in it by
adding `cognite-pygen` to the installed packages under `settings`.

```text
pyodide-http==0.2.1
cognite-sdk==7.73.3
pydantic==2.7.0
cognite-pygen==0.99.60
```

Note that we also set `pydantic` to a specific version. This is because `pygen` supports both `pydantic` `v1` and `v2`, but
when we want to use `pygen` in the CDF streamlit environment, we need to use `pydantic` `v1`.

In case you have issues with the installation, check out the [troubleshooting](../installation.html#pyodide-troubleshooting) in the
installation guide.

## Minimal Example

The following is a minimal example of a Streamlit app that uses `pygen` to generate a Python SDK client with the
`ScenarioInstanceModel` from the [Working with TimeSeries](../examples/timeseries.html) example.

```python
import streamlit as st
from cognite.client import CogniteClient
from cognite.pygen import generate_sdk_notebook
st.title("An example app in CDF with Pygen")

client = CogniteClient()

@st.cache_data
def get_client():
  return generate_sdk_notebook(
    ("IntegrationTestsImmutable", "ScenarioInstance", "1"), client
  )

@st.cache_data
def get_scenario_instances():
  client = get_client()
  return client.scenario_instance.list().to_pandas()


st.dataframe(get_scenario_instances())
```

### Explanation

The first thing we do is to import the `CogniteClient` and `generate_sdk_notebook` from `cognite.pygen`.

```python
from cognite.client import CogniteClient
from cognite.pygen import generate_sdk_notebook
```

Then, we create a Streamlit app and a `CogniteClient` instance.

```python
st.title("An example app in CDF with Pygen")

client = CogniteClient()
```

Next, we define a function that generates a SDK for the `ScenarioInstance` model. We use the `@st.cache_data` decorator
to cache the result of the function. This means that the function will only be called once, and the result will be
cached for subsequent calls.

```python
@st.cache_data
def get_client():
  return generate_sdk_notebook(
    ("IntegrationTestsImmutable", "ScenarioInstance", "1"), client
  )
```

We then define a function that uses the generated SDK to list all `ScenarioInstance` objects in the project. We also
cache the result of this function.

```python
@st.cache_data
def get_scenario_instances():
  client = get_client()
  return client.scenario_instance.list().to_pandas()
```

Finally, we call the `get_scenario_instances` function and display the result in a dataframe.

```python
st.dataframe(get_scenario_instances())
```
