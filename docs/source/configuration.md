# Configuration Overview

MacroBase instances are driven by YAML configuration files that describe the
query, data source, and analysis settings. The default files under `conf/`
provide ready-to-run examples for batch analytics, the streaming server, and the
REST UI.【F:conf/batch.yaml†L1-L22】【F:conf/macrobase.yaml†L1-L12】

Each configuration is divided into the following sections:

- **Pipeline selection** – `macrobase.pipeline.class` controls which pipeline is
  instantiated when running from the CLI or REST server. The bundled `batch.yaml`
  file uses the basic batched pipeline and sets a human-readable query name for
  logging.【F:conf/batch.yaml†L1-L6】
- **Loader properties** – Attributes, metrics, and data source details live under
  the `macrobase.loader.*` namespace. For SQL workloads you can provide a base
  query, credentials, and optional caching paths. Switching to CSV simply
  requires updating the loader type and pointing at a local file path.【F:conf/batch.yaml†L5-L18】【F:docs/source/user-guide/advanced-configuration.md†L57-L104】
- **Analysis options** – Classifier and summarizer thresholds live under the
  `macrobase.analysis.*` prefix. Percentile targets, minimum support, and
  risk-ratio constraints are defined here so you can tune sensitivity without
  touching the code.【F:conf/batch.yaml†L9-L18】【F:docs/source/user-guide/parameters.md†L7-L86】
- **Logging** – The `logging` block adjusts log levels for the server and CLI,
  enabling more verbose traces when debugging pipelines.【F:conf/batch.yaml†L18-L22】【F:conf/macrobase.yaml†L5-L12】

Refer to the [Parameter Reference](user-guide/parameters.md) for a catalogue of
all supported keys, their defaults, and detailed descriptions. Parameters can be
overridden on the command line by passing `-D` flags via `JAVA_OPTS`, which the
provided shell scripts pick up automatically.【F:docs/source/user-guide/advanced-configuration.md†L33-L52】

When introducing new configuration options, add them to `PipelineConfig` so they
benefit from type-safe retrieval and default handling across every entry point.【F:core/src/main/java/edu/stanford/futuredata/macrobase/pipeline/PipelineConfig.java†L11-L91】
