# Tutorial: Interpreting MacroBase Explanations

MacroBase explanations describe which attribute combinations distinguish the
outlier population from the background distribution. This guide explains how to
read the command-line output and how to extract the same information
programmatically.

## 1. Understand the output structure

Every pipeline returns an `Explanation` object whose `prettyPrint()` method emits
an "Outlier Explanation" header followed by a list of attribute groups and their
metrics.【F:lib/src/main/java/edu/stanford/futuredata/macrobase/analysis/summary/aplinear/APLExplanation.java†L37-L74】

Each entry contains three sections:

- **metrics** – Values for quality metrics such as risk ratio, z-score, or mean
  shift. They come from the `QualityMetric` array stored alongside each
  explanation result.【F:lib/src/main/java/edu/stanford/futuredata/macrobase/analysis/summary/aplinear/APLExplanationResult.java†L17-L44】
- **matches** – The attribute-value pairs that define the subgroup. These are
  decoded from the `AttributeEncoder`, so categorical identifiers remain readable
  in the final report.【F:lib/src/main/java/edu/stanford/futuredata/macrobase/analysis/summary/aplinear/APLExplanationResult.java†L46-L82】
- **aggregates** – Support counts or sums computed for the subgroup. The cube
  pipeline also includes aggregate columns such as count, mean, and standard
  deviation when available.【F:lib/src/main/java/edu/stanford/futuredata/macrobase/analysis/summary/aplinear/APLExplanationResult.java†L44-L82】

## 2. Compare outlier and total counts

The explanation header reports the total number of rows processed and how many
were marked as outliers. Use these values to sanity check whether your percentile
or predicate threshold is too strict or too permissive.【F:lib/src/main/java/edu/stanford/futuredata/macrobase/analysis/summary/aplinear/APLExplanation.java†L37-L60】

## 3. Export results to a DataFrame

When you need structured access, call `toDataFrame()` on the explanation. MacroBase
will materialize string columns for each attribute and numeric columns for every
metric and aggregate, enabling further analysis or visualization in downstream
tools.【F:lib/src/main/java/edu/stanford/futuredata/macrobase/analysis/summary/aplinear/APLExplanation.java†L62-L124】

## 4. Iterate on the workload

If no explanation meets your expectations, revisit the configuration:

- Tighten `macrobase.analysis.minSupport` to focus on larger cohorts or loosen it
  to surface rare behaviors.【F:docs/source/user-guide/parameters.md†L40-L65】
- Switch summarizers (Apriori, FP-Growth, count mean shift) to balance precision
  and runtime.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/BasicBatchPipeline.java†L122-L175】
- Revisit classifier settings to control how many rows are considered outliers
  before summarization.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/BasicBatchPipeline.java†L49-L92】

With a few iterations you will converge on explanations that isolate the true
root causes behind anomalous behavior.
