Changes
=======

This is a record of all past pytask-parallel releases and what went into them in reverse
chronological order. Releases follow `semantic versioning <https://semver.org/>`_ and
all releases are available on `PyPI <https://pypi.org/project/pytask-parallel>`_ and
`Anaconda.org <https://anaconda.org/conda-forge/pytask-parallel>`_.


0.1.0 - 2021-07-20
------------------

- :gh:`19` adds ``conda-forge`` to the ``README.rst``.
- :gh:`22` add note that the debugger cannot be used together with pytask-parallel.
- :gh:`24` replaces versioneer with setuptools-scm.
- :gh:`25` aborts build and prints reports on ``KeyboardInterrupt``.
- :gh:`27` enables rich tracebacks from subprocesses.


0.0.8 - 2021-03-05
------------------

- :gh:`17` fixes the unidentifiable version.


0.0.7 - 2021-03-04
------------------

- :gh:`14` fixes some post-release issues.
- :gh:`16` add dependencies to ``setup.py`` and changes the default backend to ``loky``.


0.0.6 - 2021-02-27
------------------

- :gh:`12` replaces all occurrences of ``n_processes`` with ``n_workers``.
- :gh:`13` adds a license, versioneer, and allows publishing on PyPI.


0.0.5 - 2020-12-28
------------------

- :gh:`5` fixes the CI and other smaller issues.
- :gh:`8` aligns pytask-parallel with task priorities in pytask v0.0.11.
- :gh:`9` enables --max-failures. Closes :gh:`7`.
- :gh:`10` releases v0.0.5.


0.0.4 - 2020-10-30
------------------

- :gh:`4` implement an executor with ``loky``.


0.0.3 - 2020-09-12
------------------

- :gh:`3` align the program with pytask v0.0.6.


0.0.2 - 2020-08-12
------------------

- :gh:`1` prepares the plugin for pytask v0.0.5.
- :gh:`2` better parsing and callbacks.


0.0.1 - 2020-07-17
------------------

- Initial commit which combined the whole effort to release v0.0.1.
