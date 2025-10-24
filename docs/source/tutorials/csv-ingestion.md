# Tutorial: CSV Ingestion Workflow

This tutorial walks through running MacroBase against a standalone CSV file from
the command line. You will configure the CSV loader, select metrics and
attributes, and inspect the resulting explanations.

## 1. Prepare the configuration

Start from the default batch configuration under `conf/batch.yaml` and update the
loader section to point at your CSV file. Set the loader type to `CSV_LOADER`
and provide the file path with a `csv://` prefix so both the CLI and GUI can
resolve the source.【F:conf/batch.yaml†L5-L18】【F:docs/source/user-guide/advanced-configuration.md†L57-L99】

```yaml
macrobase.loader.loaderType: CSV_LOADER
macrobase.loader.csv.file: csv:///absolute/path/to/sensor_readings.csv
macrobase.loader.attributes: [device_id, firmware_version]
macrobase.loader.metrics: [power_drain]
```

If your dataset stores the target metric with a lower-is-better semantics,
include it in `macrobase.analysis.metrics.lowTransform` so the classifier treats
large reciprocals as anomalies.【F:conf/batch.yaml†L5-L14】

## 2. Choose an analysis strategy

Tune the `macrobase.analysis.*` parameters to control sensitivity. The percentile
classifier marks the top tail as outliers, while `macrobase.analysis.minSupport`
and `macrobase.analysis.minOIRatio` govern how aggressive the summarizer should
be.【F:conf/batch.yaml†L9-L18】【F:docs/source/user-guide/parameters.md†L19-L74】

## 3. Execute the batch pipeline

Run the CLI wrapper to execute the pipeline with your configuration:

```bash
./bin/cli.sh conf/batch.yaml
```

The script instantiates the configured pipeline, prints loading and
classification timings, and writes the explanation to stdout.【F:core/README.md†L20-L36】

## 4. Interpret the results

The output lists attribute combinations, their support in the outlier set, and
how much more frequently they occur relative to the inliers. Use this signal to
adjust thresholds, add or remove attributes, and refine the workload-specific
configuration. Refer to the [Explanation Interpretation tutorial](explanation-interpretation.md)
for guidance on reading the summary tables.
