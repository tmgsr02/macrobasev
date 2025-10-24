# MacroBase Architecture Overview

MacroBase is organized around a modular analytics engine that loads data, identifies
outliers, and produces human-readable explanations. The core runtime is
implemented in the `core` module and centers on pipelines that coordinate the
major subsystems:

- **Ingestion** layers connect to CSV files, SQL databases, and HTTP endpoints
  to materialize `DataFrame` objects that carry typed columns into the engine.
  The batch pipeline delegates all loading through `PipelineUtils.loadDataFrame`
  so every run enforces schema expectations before execution.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/BasicBatchPipeline.java†L94-L120】
- **Classification** stages isolate outliers. The standard batch execution path
  supports percentile, predicate, and count mean shift classifiers, each of
  which implements a shared interface so they can be configured at runtime.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/BasicBatchPipeline.java†L37-L92】
- **Summarization** components turn classified data into explanations. The
  pipeline can switch between Apriori-style summarization, FP-Growth mining, and
  count mean shift summaries depending on the workload requirements.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/BasicBatchPipeline.java†L122-L175】

The cubed pipeline extends this architecture for pre-aggregated datasets. It
accepts additional metadata such as mean, standard deviation, and quantile
columns while preserving the same load → classify → summarize flow.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/CubePipeline.java†L21-L125】【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/CubePipeline.java†L158-L217】

## Deployment Surfaces

MacroBase ships with multiple entry points that reuse the shared pipeline
primitives:

- **Command line tools** rely on YAML configuration files that describe the
  ingestion source, classifier, and summarizer parameters. Scripts under `bin/`
  forward the configuration to the CLI layer, which instantiates the requested
  pipeline and prints the resulting explanations.【F:core/README.md†L1-L36】
- **REST server** boots the same pipelines behind HTTP so they can be triggered
  remotely. The default demonstration starts the service with `bin/server.sh`
  and submits a query using the sample shell script.【F:core/README.md†L38-L53】
- **SQL and GUI front ends** leverage the REST API to surface configuration
  controls, allowing interactive exploration without touching YAML files. The
  tutorials in this documentation explain how to connect each interface.

## Configuration Model

All configuration flows through `PipelineConfig`, which treats YAML content as
structured lookups with sensible defaults. Pipelines read the required values
(e.g., `inputURI`, classifier-specific thresholds, attribute lists) from this
abstraction so adding new options requires minimal boilerplate.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/PipelineConfig.java†L11-L91】

## Extensibility

Because the ingestion, classification, and summarization stages are
independently pluggable, MacroBase can be extended by implementing new
interfaces in each layer:

- Add a new loader by implementing the `DataFrameLoader` contract and wiring it
  into `PipelineUtils.loadDataFrame`.
- Extend classification by creating a `Classifier` subclass and mapping it in
  the pipeline switch statements.
- Deliver alternative explanations by adding a `BatchSummarizer` implementation
  and registering it via configuration.

These seams allow teams to specialize MacroBase for domain-specific anomaly
workloads without rewriting the orchestration logic.
