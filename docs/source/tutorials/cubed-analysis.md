# Tutorial: Cubed Data Analysis

Pre-aggregated cubes capture counts and statistics per attribute combination.
MacroBase's cube pipeline consumes these aggregates directly so you can analyze
large datasets without re-scanning the raw events.

## 1. Inspect the sample cube configuration

The repository ships with `core/demo/cube.json`, which demonstrates the required
fields for a cube workload: pipeline name, input URI, classifier family, and the
aggregate columns required by the classifier.【F:core/demo/cube.json†L1-L16】

```json
{
  "pipeline": "CubePipeline",
  "inputURI": "csv://core/demo/sample_cubed.csv",
  "classifier": "arithmetic",
  "countColumn": "count",
  "meanColumn": "mean",
  "stdColumn": "std",
  "attributes": ["location", "version"],
  "minSupport": 0.2,
  "minRatioMetric": 10.0
}
```

Use this file as a template for your own cubes by updating the attribute names
and thresholds.

## 2. Configure classifier-specific inputs

Different classifiers expect different statistics:

- Arithmetic and mean shift classifiers read `meanColumn` and `stdColumn` to
  compute z-scores for each cube row.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/CubePipeline.java†L67-L93】【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/CubePipeline.java†L129-L155】
- Count mean shift optionally evaluates string predicates when the cutoff is a
  string; otherwise it uses numerical thresholds on the metric column.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/CubePipeline.java†L33-L65】
- Quantile classifiers rely on a map of quantile columns and their expected
  values to detect tail deviations.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/CubePipeline.java†L78-L110】

Set the relevant keys in your JSON configuration so the pipeline can enforce the
correct schema before loading the cube.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/CubePipeline.java†L129-L155】

## 3. Run the cube pipeline

Invoke the CLI with the cube configuration:

```bash
./bin/cli.sh core/demo/cube.json
```

The pipeline loads the cube, applies the configured classifier, and then feeds
its output to an APL summarizer that ranks attribute combinations by support and
risk ratio.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/CubePipeline.java†L158-L217】

## 4. Explore the explanation

The resulting `APLExplanation` reports the highest-impact attribute groups. Use
`debugDump: true` during experimentation to emit the classified cube to
`classified.csv` for further inspection.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/CubePipeline.java†L95-L124】
