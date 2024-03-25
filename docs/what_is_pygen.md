# What is `Pygen`?

You have created your data model and want to start writing Python code to interact with it. Then, you have multiple
options

1. You can use `GraphQL`, and call the endpoint `https://{cluster}.cognitedata.com/api/v1/projects/{project}/userapis/spaces/{space}/datamodels/{externalId}/versions/{version}/graphql`.
   * This gives you a flexible way to query your data model, but
     - writing the queries can be cumbersome.
     - the response is a dictionary that you need to parse.
     - you need to know the data model structure.
2. You can use the Data Modeling Storage, `DMS` endpoint `https://{cluster}.cognitedata.com/api/v1/projects/{project}/models/instances/`.
   * This lacks the context of your data model
     - you are using a set of generic endpoints designed to work with any data model.
     - the response is a dictionary that you need to parse.
     - you need to know the data model structure.

`Pygen` is offering a third option. It generates Python code that wraps the `DMS` endpoint with your
data model. This way, you can interact with your data model using Python objects, which gives you the following benefits:

1. You can interact with your data model using Python objects.
2. Your IDE can provide you with code completion and type hints for your data model.
3. Client-side validation of data when creating or updating objects.
4. Enable you to work in the language of your data model.

# What does `Pygen` do?

The input to `Pygen` are the views of the data model.

## Data Class


## API Class
