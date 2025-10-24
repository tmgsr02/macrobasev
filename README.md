## MacroBase 

[![Build Status](https://travis-ci.org/stanford-futuredata/macrobase.svg)](https://travis-ci.org/stanford-futuredata/macrobase)
[![Coverage Status](https://coveralls.io/repos/github/stanford-futuredata/macrobase/badge.svg?branch=master)](https://coveralls.io/github/stanford-futuredata/macrobase?branch=master)

MacroBase is a data analytics tool that prioritizes attention in large datasets using machine learning.

For tutorial, documentation, papers and additional information, please refer to our project website: http://macrobase.stanford.edu/.

### Python analysis utilities

This repository now exposes a distributable Python package named
``macrobasev`` that contains the analysis utilities previously found under the
``tools`` directory. The package can be built locally with::

    python -m build

which will generate both a source distribution and a wheel under ``dist/``.
After building you can inspect the artifacts with ``twine check dist/*`` and,
once satisfied, publish them to PyPI as described in
``docs/release-checklist.md``.
