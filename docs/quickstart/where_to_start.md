`pygen` has multiple use cases spanning different user skill levels. This section will help you find the right place to start.

The minimum requirements for using `pygen` is to have a Cognite Data Fusion (CDF) project available. Note that `pygen` is
made for people with a very basic understanding of Python, but instead have domain knowledge of the data they are
exploring and analyzing.

For the more advanced users, `pygen` is a great tool for building solutions quickly by tailoring the generated code to
your data model. Thus making it easy to get data in a format that is close to work with for your specific use case. It
is an alternative to `graphql` that is more pythonic.

## (Beginner) Exploration
If you are just curious about `pygen` and all you have is a CDF project, and you don't even have a data model or data
a great place to start is in the CDF notebook with a demo model packaged into `pygen`,
see [Generating SDK using Demo Data Model](cdf_notebook.html#generating-sdk-using-demo-data-model). The advantage of using
a CDF notebook is that you do not have to know how to install and setup `Python`.

Do you have your own data model you want to explore using the CDF notebook, then see [Explore in Notebook](cdf_notebook.html)

If you know how to install and setup `Python` and you have a CDF project, and prefer to work in your own environment,
then you can start with the [Local Notebook](notebook.html) guide.

Exploration is also useful in the data modeling process to quickly explore a data model and, for example, try
how it can be queried, and whether it will support a specific use case.

## (Intermediate) Building a Solution
If you have a CDF project and you want to build a solution, for example, a dashboard using `plotly` or `dash`, or
a machine learning model workflow using `scikit-learn` or `tensorflow`, then you should generate a `pygen` SDK and
check it into git history. See the [Project](project.html) for more information.

## (Intermediate) Ingestion Data into CDF
`pygen` can also be used to create an extractor that is used to ingest data into CDF. The typical use case
is when you have a data source that is creating nested data and you want to perform client side validation of the
data before ingesting it into CDF. See the [Data Population](ingestion.html) for more information.

## (Intermediate) Migration Data between Data Models
`pygen` can also be used to move data from one model to another. In short, you create an SDK using `pygen` for each
data model, and then write the code to move data between the models. See the [Data migration](migration.html) for more information.
