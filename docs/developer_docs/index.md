# Developer Documentation

This section contains the developer documentation for the project. It is intended for developers who want to
contribute to the project or understand how it works.

!!! warning "Internal Documentation"
    This documentation contains the internal structure of the project. Internal modules, classes, and functions
    are subject to change without notice. If you are a user of the project, please
    refer to the [User Documentation](../api/api.md) for the public classes and functions which are stable.

The basic idea of Pygen is as follows:

1. Input Views in Read Format (Pygen does know about containers).
2. Converts these views into Pygen internal representation, located in the `pygen._core.models` module.
3. Use the internal representation as input to the Jinja templates, located in the `pygen.templates` module.
4. The logic for combining the internal representation with the Jinja templates is located in the `pygen.generator` module.
