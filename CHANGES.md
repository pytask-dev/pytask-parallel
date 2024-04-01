# Changes

This is a record of all past pytask-parallel releases and what went into them in reverse
chronological order. Releases follow [semantic versioning](https://semver.org/) and all
releases are available on [PyPI](https://pypi.org/project/pytask-parallel) and
[Anaconda.org](https://anaconda.org/conda-forge/pytask-parallel).

## 0.5.0 - 2024-xx-xx

- {pull}`85` simplifies code since loky is a dependency.
- {pull}`86` adds support for dask.
- {pull}`89` restructures the package.

## 0.4.1 - 2024-01-12

- {pull}`72` moves the project to `pyproject.toml`.
- {pull}`75` updates the release strategy.
- {pull}`79` add tests for Jupyter and fix parallelization with `PythonNode`s.
- {pull}`80` adds support for partialed functions.
- {pull}`82` fixes testing with pytask v0.4.5.

## 0.4.0 - 2023-10-07

- {pull}`62` deprecates Python 3.7.
- {pull}`64` aligns pytask-parallel with pytask v0.4.0rc2.
- {pull}`66` deactivates parallelization for dry-runs.
- {pull}`67` fixes parallelization with partialed task functions.
- {pull}`68` raises more informative error message when `breakpoint()` was uses when
  parallelizing with processes or loky.

## 0.3.1 - 2023-05-27

- {pull}`56` refactors the `ProcessPoolExecutor`.
- {pull}`57` does some housekeeping.
- {pull}`59` sets the default backend to `ProcessPoolExecutor` even when loky is
  installed.

## 0.3.0 - 2023-01-23

- {pull}`50` deprecates INI configurations and aligns the package with pytask v0.3.
- {pull}`51` adds ruff and refurb.

## 0.2.1 - 2022-08-19

- {pull}`43` adds docformatter.
- {pull}`44` allows to capture warnings from subprocesses. Fixes {issue}`41`.
- {pull}`45` replaces the delay command line option with an internal, dynamic parameter.
  Fixes {issue}`41`.
- {pull}`46` adds a dynamic sleep duration during the execution. Fixes {issue}`42`.

## 0.2.0 - 2022-04-15

- {pull}`31` adds types to the package.
- {pull}`36` adds a test for <https://github.com/pytask-dev/pytask/issues/216>.
- {pull}`37` aligns pytask-parallel with pytask v0.2.

## 0.1.1 - 2022-02-08

- {pull}`30` removes unnecessary content from `tox.ini`.
- {pull}`33` skips concurrent CI builds.
- {pull}`34` deprecates Python 3.6 and adds support for Python 3.10.

## 0.1.0 - 2021-07-20

- {pull}`19` adds `conda-forge` to the `README.rst`.
- {pull}`22` add note that the debugger cannot be used together with pytask-parallel.
- {pull}`24` replaces versioneer with setuptools-scm.
- {pull}`25` aborts build and prints reports on `KeyboardInterrupt`.
- {pull}`27` enables rich tracebacks from subprocesses.

## 0.0.8 - 2021-03-05

- {pull}`17` fixes the unidentifiable version.

## 0.0.7 - 2021-03-04

- {pull}`14` fixes some post-release issues.
- {pull}`16` add dependencies to `setup.py` and changes the default backend to `loky`.

## 0.0.6 - 2021-02-27

- {pull}`12` replaces all occurrences of `n_processes` with `n_workers`.
- {pull}`13` adds a license, versioneer, and allows publishing on PyPI.

## 0.0.5 - 2020-12-28

- {pull}`5` fixes the CI and other smaller issues.
- {pull}`8` aligns pytask-parallel with task priorities in pytask v0.0.11.
- {pull}`9` enables --max-failures. Closes {issue}`7`.
- {pull}`10` releases v0.0.5.

## 0.0.4 - 2020-10-30

- {pull}`4` implement an executor with `loky`.

## 0.0.3 - 2020-09-12

- {pull}`3` align the program with pytask v0.0.6.

## 0.0.2 - 2020-08-12

- {pull}`1` prepares the plugin for pytask v0.0.5.
- {pull}`2` better parsing and callbacks.

## 0.0.1 - 2020-07-17

- Initial commit which combined the whole effort to release v0.0.1.
