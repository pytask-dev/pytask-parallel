.. image:: https://img.shields.io/pypi/v/pytask-parallel?color=blue
    :alt: PyPI
    :target: https://pypi.org/project/pytask-parallel

.. image:: https://img.shields.io/pypi/pyversions/pytask-parallel
    :alt: PyPI - Python Version
    :target: https://pypi.org/project/pytask-parallel

.. image:: https://img.shields.io/conda/vn/conda-forge/pytask-parallel.svg
    :target: https://anaconda.org/conda-forge/pytask-parallel

.. image:: https://img.shields.io/conda/pn/conda-forge/pytask-parallel.svg
    :target: https://anaconda.org/conda-forge/pytask-parallel

.. image:: https://img.shields.io/pypi/l/pytask-parallel
    :alt: PyPI - License
    :target: https://pypi.org/project/pytask-parallel

.. image:: https://img.shields.io/github/workflow/status/pytask-dev/pytask-parallel/Continuous%20Integration%20Workflow/main
   :target: https://github.com/pytask-dev/pytask-parallel/actions?query=branch%3Amain

.. image:: https://codecov.io/gh/pytask-dev/pytask-parallel/branch/main/graph/badge.svg
    :target: https://codecov.io/gh/pytask-dev/pytask-parallel

.. image:: https://results.pre-commit.ci/badge/github/pytask-dev/pytask-parallel/main.svg
    :target: https://results.pre-commit.ci/latest/github/pytask-dev/pytask-parallel/main
    :alt: pre-commit.ci status

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/psf/black


------

pytask-parallel
===============

Parallelize the execution of tasks with ``pytask-parallel`` which is a plugin for
`pytask <https://github.com/pytask-dev/pytask>`_.


Installation
------------

pytask-parallel is available on `PyPI <https://pypi.org/project/pytask-parallel>`_ and
`Anaconda.org <https://anaconda.org/conda-forge/pytask-parallel>`_. Install it with

.. code-block:: console

    $ pip install pytask-parallel

    # or

    $ conda install -c conda-forge pytask-parallel

By default, the plugin uses ``loky``'s robust implementation of the
``ProcessPoolExecutor``.

It is also possible to select the ``ProcessPoolExecutor`` or ``ThreadPoolExecutor`` from
the `concurrent.futures <https://docs.python.org/3/library/concurrent.futures.html>`_
module as backends to execute tasks asynchronously.


Usage
-----

To parallelize your tasks across many workers, pass an integer greater than 1 or
``'auto'`` to the command-line interface.

.. code-block:: console

    $ pytask -n 2
    $ pytask --n-workers 2

    # Starts os.cpu_count() - 1 workers.
    $ pytask -n auto


Using processes to parallelize the execution of tasks is useful for CPU bound tasks such
as numerical computations. (`Here <https://stackoverflow.com/a/868577/7523785>`_ is an
explanation on what CPU or IO bound means.)

For IO bound tasks, tasks where the limiting factor are network responses, access to
files, you can parallelize via threads.

.. code-block:: console

    $ pytask --parallel-backend threads

You can also set the options in one of the configuration files (``pytask.ini``,
``tox.ini``, or ``setup.cfg``).

.. code-block:: ini

    # This is the default configuration. Note that, parallelization is turned off.

    [pytask]
    n_workers = 1
    parallel_backend = loky  # or processes or threads

.. warning::

    It is not possible to combine parallelization with debugging. That is why ``--pdb``
    or ``--trace`` deactivate parallelization.

    If you parallelize the execution of your tasks using two or more workers, do not use
    ``breakpoint()`` or ``import pdb; pdb.set_trace()`` since both will cause
    exceptions.


Changes
-------

Consult the `release notes <CHANGES.rst>`_ to find out about what is new.


Development
-----------

- ``pytask-parallel`` does not call the ``pytask_execute_task_protocol`` hook
  specification/entry-point because ``pytask_execute_task_setup`` and
  ``pytask_execute_task`` need to be separated from ``pytask_execute_task_teardown``.
  Thus, plugins which change this hook specification may not interact well with the
  parallelization.

- There are two PRs for CPython which try to re-enable setting custom reducers which
  should have been working, but does not. Here are the references.

    + https://bugs.python.org/issue28053
    + https://github.com/python/cpython/pull/9959
    + https://github.com/python/cpython/pull/15058
