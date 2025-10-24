# Pipeline Catalogue

MacroBase exposes several pre-built pipelines that orchestrate ingestion,
classification, and summarization. This guide explains how each pipeline is
configured and what assumptions it makes about the incoming data.

## Basic Batch Pipeline

The batch pipeline is the default choice for row-oriented datasets. It expects a
single metric column, one or more categorical attributes, and optional
precomputed means for count mean shift scenarios. The constructor pulls runtime
options such as classifier type, thresholds, and attribute lists from the
`PipelineConfig` abstraction, so every aspect can be tuned in YAML.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/BasicBatchPipeline.java†L24-L90】

Execution follows three steps:

1. Load typed columns via `PipelineUtils.loadDataFrame`, validating that the
   configured metrics and attributes are present before any computation
   begins.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/BasicBatchPipeline.java†L94-L120】
2. Run the selected classifier to flag outliers. Percentile and predicate
   classifiers operate on raw metric values, while count mean shift requires the
   associated mean column for context.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/BasicBatchPipeline.java†L49-L92】
3. Summarize outliers using either Apriori-style, FP-Growth, or count mean shift
   summarizers, all configured with the same attribute list and support
   thresholds.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/BasicBatchPipeline.java†L122-L175】

The `results()` method records loading and classification timings, then returns
an `Explanation` object containing the ranked attribute combinations that best
describe the outlier population.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/BasicBatchPipeline.java†L177-L226】

## Cube Pipeline

Cubed data contains pre-aggregated statistics, so the pipeline accepts count,
mean, standard deviation, and quantile columns alongside attribute dimensions.
Configuration options include the classifier family, count column name, and
flags for predicate or percentile logic.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/CubePipeline.java†L27-L110】

During execution the pipeline:

1. Determines the schema requirements for the chosen classifier, ensuring that
   expected aggregate columns are supplied.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/CubePipeline.java†L129-L155】
2. Ingests the cube and runs a `CubeClassifier`, which can interpret metric
   columns as predicates, arithmetic mean shifts, or quantile thresholds.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/CubePipeline.java†L158-L205】
3. Produces an `APLExplanation` summarizing attribute groups that carry abnormal
   aggregate behavior.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/CubePipeline.java†L205-L217】

Optional debugging can write the classified cube back to disk before
summarization when `debugDump` is enabled.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/CubePipeline.java†L95-L124】

## Choosing a Pipeline

| Scenario | Recommended Pipeline | Key Configuration |
| --- | --- | --- |
| Raw metric streams with categorical attributes | Basic batch | `metric`, `attributes`, `classifier`, `summarizer` |
| Pre-aggregated cubes with counts and statistics | Cube | `countColumn`, `metric`, `meanColumn`, classifier-specific thresholds |
| Custom ingestion or summarization | Implement a new pipeline by composing loaders, classifiers, and summarizers | Extend `Pipeline` interface |

When extending MacroBase, inherit from the `Pipeline` interface so custom flows
return explanations compatible with the rest of the ecosystem.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/Pipeline.java†L5-L24】
