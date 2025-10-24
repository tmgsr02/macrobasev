# MacroBase Documentation

MacroBase is an analytic monitoring engine designed to prioritize attention in
large-scale datasets and data streams. It focuses on detecting unusual behavior
and explaining which attribute combinations best describe each anomaly cohort.

Use the navigation sidebar to explore:

- The [architecture overview](architecture.md) for a tour of the ingestion,
  classification, and summarization layers that power every pipeline.
- Detailed [pipeline guides](pipelines.md) that explain when to use the batch or
  cube execution paths and how to tune them for your workloads.
- A [configuration reference](configuration.md) and full [parameter catalogue](user-guide/parameters.md)
  covering every YAML key supported by the engine.
- Tutorials that mirror common workflows, including [CSV ingestion](tutorials/csv-ingestion.md),
  [cubed data analysis](tutorials/cubed-analysis.md), and [explanation interpretation](tutorials/explanation-interpretation.md).
- Automatically generated [API documentation](api/command-line.md) for the Python
  wrappers that script MacroBase's command line tools.

If you're experimenting with the SQL or GUI front ends, revisit the existing
walkthroughs to learn how to connect your databases and explore explanations
interactively.
