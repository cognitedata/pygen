## Installation Local
**Prerequisites**: Installed Python 3.9 or newer, see [python.org](https://www.python.org/downloads/)

1. Create a virtual environment.
2. Activate your virtual environment.
3. Install `cognite-pygen` with the all option.

=== "Windows"

    ```
    python -m venv venv
    venv\Scripts\activate.bat
    pip install cognite-pygen[all]
    ```

=== "Mac/Linux"

    ```bash
    python -m venv venv
    source venv/bin/activate
    pip install "cognite-pygen[all]"
    ```

## Installation Options

=== "No extras"

    ```
    pip install cognite-pygen
    ```

    This only installs the core dependencies for `cognite-pygen` and is useful if you only want to generate the SDK
    from a Python script, for example, in a notebook.

=== "cli"

    ```
    pip install cognite-pygen[cli]
    ```

    This installs the core dependencies for `cognite-pygen` and the dependencies for the CLI. This is useful if you
    want to generate the SDK from the command line.

=== "format"

    ```
    pip install cognite-pygen[format]
    ```

    This installs the core dependencies for `cognite-pygen` and the dependencies for formatting the generated SDK.
    This is useful if you want to format the generated SDK code with black.

=== "all"

    ```
    pip install cognite-pygen[all]
    ```

    This installs the core dependencies for `cognite-pygen`, as well as the CLI and code formatting dependencies.


## Pyodide Installation

In a `CDF Notebook` you can install `cognite-pygen` by running the following code in a cell:

```python
%pip install cognite-pygen
```

## Pyodide Troubleshooting

### Can't find a pure Python 3 wheel: 'cognite-pygen'

**Context Seen**: CDF Streamlit environment

??? Error

    Traceback (most recent call last):
    File "/lib/python3.11/site-packages/micropip/_commands/install.py", line 146, in install
    raise ValueError(
          ValueError: Can't find a pure Python 3 wheel for: 'cognite-pygen==0.99.17'
          See: https://pyodide.org/en/stable/usage/faq.html#why-can-t-micropip-find-a-pure-python-wheel-for-a-package
    )

This error can occur if one of the dependencies of `cognite-pygen` is violated. For example, if you have
```requirements
cognite-sdk==7.5.0
cognite-pygen==0.99.17
```
You will get the error above because `cognite-pygen` requires the `cognite-sdk` version to be `>=7.13.6`. You
can fix this by updating the `cognite-sdk` version to `>=7.13.6` in the `requirements.txt` file.

```requirements
cognite-sdk==7.13.6
cognite-pygen==0.99.17
```

### Requested 'typing-extensions>=4.10.0; python_version < "3.13"', but typing-extensions==4.7.1 is already installed

**Context Seen**: CDF Notebook Environment


??? Error

    ValueError                                Traceback (most recent call last)
    Cell In[2], line 1
    ----> 1 await __import__("piplite").install(**{'requirements': ['cognite-pygen==0.99.16']})
          2 from cognite import pygen
          3 print(pygen.__version__)

    File /lib/python3.11/site-packages/piplite/piplite.py:117, in _install(requirements, keep_going, deps, credentials, pre, index_urls, verbose)
        115 """Invoke micropip.install with a patch to get data from local indexes"""
        116 with patch("micropip.package_index.query_package", _query_package):
    --> 117     return await micropip.install(
        118         requirements=requirements,
        119         keep_going=keep_going,
        120         deps=deps,
        121         credentials=credentials,
        122         pre=pre,
        123         index_urls=index_urls,
        124         verbose=verbose,
        125     )

    File /lib/python3.11/site-packages/micropip/_commands/install.py:142, in install(requirements, keep_going, deps, credentials, pre, index_urls, verbose)
        130     index_urls = package_index.INDEX_URLS[:]
        132 transaction = Transaction(
        133     ctx=ctx,
        134     ctx_extras=[],
       (...)
        140     index_urls=index_urls,
        141 )
    --> 142 await transaction.gather_requirements(requirements)
        144 if transaction.failed:
        145     failed_requirements = ", ".join([f"'{req}'" for req in transaction.failed])

    File /lib/python3.11/site-packages/micropip/transaction.py:204, in Transaction.gather_requirements(self, requirements)
        201 for requirement in requirements:
        202     requirement_promises.append(self.add_requirement(requirement))
    --> 204 await asyncio.gather(*requirement_promises)

    File /lib/python3.11/site-packages/micropip/transaction.py:211, in Transaction.add_requirement(self, req)
        208     return await self.add_requirement_inner(req)
        210 if not urlparse(req).path.endswith(".whl"):
    --> 211     return await self.add_requirement_inner(Requirement(req))
        213 # custom download location
        214 wheel = WheelInfo.from_url(req)

    File /lib/python3.11/site-packages/micropip/transaction.py:300, in Transaction.add_requirement_inner(self, req)
        297     if self._add_requirement_from_pyodide_lock(req):
        298         return
    --> 300     await self._add_requirement_from_package_index(req)
        301 else:
        302     try:

    File /lib/python3.11/site-packages/micropip/transaction.py:347, in Transaction._add_requirement_from_package_index(self, req)
        344 if satisfied:
        345     logger.info(f"Requirement already satisfied: {req} ({ver})")
    --> 347 await self.add_wheel(wheel, req.extras, specifier=str(req.specifier))

    File /lib/python3.11/site-packages/micropip/transaction.py:385, in Transaction.add_wheel(self, wheel, extras, specifier)
        383 await wheel.download(self.fetch_kwargs)
        384 if self.deps:
    --> 385     await self.gather_requirements(wheel.requires(extras))
        387 self.wheels.append(wheel)

    File /lib/python3.11/site-packages/micropip/transaction.py:204, in Transaction.gather_requirements(self, requirements)
        201 for requirement in requirements:
        202     requirement_promises.append(self.add_requirement(requirement))
    --> 204 await asyncio.gather(*requirement_promises)

    File /lib/python3.11/site-packages/micropip/transaction.py:208, in Transaction.add_requirement(self, req)
        206 async def add_requirement(self, req: str | Requirement) -> None:
        207     if isinstance(req, Requirement):
    --> 208         return await self.add_requirement_inner(req)
        210     if not urlparse(req).path.endswith(".whl"):
        211         return await self.add_requirement_inner(Requirement(req))

    File /lib/python3.11/site-packages/micropip/transaction.py:300, in Transaction.add_requirement_inner(self, req)
        297     if self._add_requirement_from_pyodide_lock(req):
        298         return
    --> 300     await self._add_requirement_from_package_index(req)
        301 else:
        302     try:

    File /lib/python3.11/site-packages/micropip/transaction.py:347, in Transaction._add_requirement_from_package_index(self, req)
        344 if satisfied:
        345     logger.info(f"Requirement already satisfied: {req} ({ver})")
    --> 347 await self.add_wheel(wheel, req.extras, specifier=str(req.specifier))

    File /lib/python3.11/site-packages/micropip/transaction.py:385, in Transaction.add_wheel(self, wheel, extras, specifier)
        383 await wheel.download(self.fetch_kwargs)
        384 if self.deps:
    --> 385     await self.gather_requirements(wheel.requires(extras))
        387 self.wheels.append(wheel)

    File /lib/python3.11/site-packages/micropip/transaction.py:204, in Transaction.gather_requirements(self, requirements)
        201 for requirement in requirements:
        202     requirement_promises.append(self.add_requirement(requirement))
    --> 204 await asyncio.gather(*requirement_promises)

    File /lib/python3.11/site-packages/micropip/transaction.py:208, in Transaction.add_requirement(self, req)
        206 async def add_requirement(self, req: str | Requirement) -> None:
        207     if isinstance(req, Requirement):
    --> 208         return await self.add_requirement_inner(req)
        210     if not urlparse(req).path.endswith(".whl"):
        211         return await self.add_requirement_inner(Requirement(req))

    File /lib/python3.11/site-packages/micropip/transaction.py:300, in Transaction.add_requirement_inner(self, req)
        297     if self._add_requirement_from_pyodide_lock(req):
        298         return
    --> 300     await self._add_requirement_from_package_index(req)
        301 else:
        302     try:

    File /lib/python3.11/site-packages/micropip/transaction.py:347, in Transaction._add_requirement_from_package_index(self, req)
        344 if satisfied:
        345     logger.info(f"Requirement already satisfied: {req} ({ver})")
    --> 347 await self.add_wheel(wheel, req.extras, specifier=str(req.specifier))

    File /lib/python3.11/site-packages/micropip/transaction.py:385, in Transaction.add_wheel(self, wheel, extras, specifier)
        383 await wheel.download(self.fetch_kwargs)
        384 if self.deps:
    --> 385     await self.gather_requirements(wheel.requires(extras))
        387 self.wheels.append(wheel)

    File /lib/python3.11/site-packages/micropip/transaction.py:204, in Transaction.gather_requirements(self, requirements)
        201 for requirement in requirements:
        202     requirement_promises.append(self.add_requirement(requirement))
    --> 204 await asyncio.gather(*requirement_promises)

    File /lib/python3.11/site-packages/micropip/transaction.py:208, in Transaction.add_requirement(self, req)
        206 async def add_requirement(self, req: str | Requirement) -> None:
        207     if isinstance(req, Requirement):
    --> 208         return await self.add_requirement_inner(req)
        210     if not urlparse(req).path.endswith(".whl"):
        211         return await self.add_requirement_inner(Requirement(req))

    File /lib/python3.11/site-packages/micropip/transaction.py:290, in Transaction.add_requirement_inner(self, req)
        287 # Is some version of this package is already installed?
        288 req.name = canonicalize_name(req.name)
    --> 290 satisfied, ver = self.check_version_satisfied(req)
        291 if satisfied:
        292     logger.info(f"Requirement already satisfied: {req} ({ver})")

    File /lib/python3.11/site-packages/micropip/transaction.py:235, in Transaction.check_version_satisfied(self, req)
        231 if req.specifier.contains(ver, prereleases=True):
        232     # installed version matches, nothing to do
        233     return True, ver
    --> 235 raise ValueError(
        236     f"Requested '{req}', " f"but {req.name}=={ver} is already installed"
        237 )

    ValueError: Requested 'typing-extensions>=4.10.0; python_version < "3.13"', but typing-extensions==4.7.1 is already installed

This is a known bug in `pyodide`, GitHub issue [pyodide#4234](https://github.com/pyodide/pyodide/issues/4234).

The workaround is to manually uninstall the `typing-extensions` package and then install the `cognite-pygen` package.

In a cell, run the following code:

```python
import micropip
micropip.uninstall('typing_extensions')
```

Then, install `cognite-pygen`:

```python
%pip install cognite-pygen
```

In a `CDF Streamlit` environment, you can install the `typing-extensions` package before installing `cognite-pygen`:

```text
pyodide-http==0.2.1
pydantic==1.10.7
typing-extensions>=4.10.0
cognite-sdk==7.54.4
cognite-pygen==0.99.27
```
