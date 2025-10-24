## MacroBase 

[![Build Status](https://travis-ci.org/stanford-futuredata/macrobase.svg)](https://travis-ci.org/stanford-futuredata/macrobase)
[![Coverage Status](https://coveralls.io/repos/github/stanford-futuredata/macrobase/badge.svg?branch=master)](https://coveralls.io/github/stanford-futuredata/macrobase?branch=master)

MacroBase is a data analytics tool that prioritizes attention in large datasets using machine learning.

For tutorial, documentation, papers and additional information, please refer to our project website: http://macrobase.stanford.edu/.

### MacroBase Python Toolkit

This repository now includes an experimental Python package, `macrobase-py`,
that provides building blocks for pipelines, classifiers, explainers, and
visualizations inspired by the original MacroBase system. The package is
managed with [Hatch](https://hatch.pypa.io/latest/) and defined in
`pyproject.toml`.

#### Getting Started

```bash
pip install hatch
hatch env create
hatch shell
```

Once inside the environment you can install the project in editable mode:

```bash
pip install -e .
```

Optionally install Arrow support (for Parquet/Feather IO helpers) with:

```bash
pip install -e .[arrow]
```

### Tooling

* Run `pre-commit install` to enable automatic formatting (Black/isort) and
  linting (Flake8) before each commit.
* Continuous integration runs linting and the test suite across Python 3.9+
  via GitHub Actions located in `.github/workflows/python.yml`.
