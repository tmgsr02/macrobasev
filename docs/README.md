# MacroBase Documentation Toolkit

This repository uses [MkDocs](https://www.mkdocs.org/) to build the project
manual. The configuration file lives at `docs/mkdocs.yml` and the source
material is stored in `docs/source/`.

## Prerequisites

Install the documentation dependencies into your Python environment:

```bash
pip install mkdocs mkdocstrings[python]
```

## Local development

Serve the documentation locally while you iterate:

```bash
mkdocs serve --config-file docs/mkdocs.yml
```

MkDocs will watch the files under `docs/source/` and rebuild the site on every
save. To produce a static site, run:

```bash
mkdocs build --config-file docs/mkdocs.yml --site-dir docs/site
```

## Publishing

Documentation is published automatically to GitHub Pages via the `Docs` workflow
in `.github/workflows/docs.yml`. The workflow runs on pushes to the default
branch, builds the site, and deploys the rendered HTML to the `gh-pages`
branch.

You can trigger the same deployment manually with:

```bash
mkdocs gh-deploy --config-file docs/mkdocs.yml
```

The generated site will be available at
`https://stanford-futuredata.github.io/macrobase/` once the workflow completes.
