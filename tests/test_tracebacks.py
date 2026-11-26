from __future__ import annotations

import runpy
from io import StringIO
from pathlib import Path

import pytest
from distributed import Client
from distributed import LocalCluster
from pytask import ExecutionReport
from pytask import TaskWithoutPath
from rich.console import Console

from pytask_parallel.execute import _create_failure_report
from pytask_parallel.tracebacks import RemoteExecutionReport
from pytask_parallel.tracebacks import RemoteTraceback
from pytask_parallel.wrappers import WrapperResult
from pytask_parallel.wrappers import wrap_task_in_process


def _run_task_on_worker(filename, *, delete_source=False):
    task = TaskWithoutPath(name="fail", function=runpy.run_path(filename)["fail"])
    if delete_source:
        Path(filename).unlink()
    return wrap_task_in_process(
        task,
        console_options=Console(width=100).options,
        kwargs={},
        remote=True,
        session_filterwarnings=(),
        show_locals=True,
        task_filterwarnings=(),
    )


def _render(report):
    out = StringIO()
    Console(file=out, width=240, color_system=None).print(report)
    return out.getvalue()


@pytest.fixture(scope="module")
def dask_client():
    with (
        LocalCluster(
            n_workers=1, threads_per_worker=1, processes=True, dashboard_address=None
        ) as cluster,
        Client(cluster) as client,
    ):
        yield client


@pytest.mark.parametrize("delete_source", [False, True])
def test_remote_traceback_preserves_path_and_uses_worker_source(
    dask_client, tmp_path, delete_source
):
    source = (
        "def fail():\n"
        "    worker_local = 'remote-' + 'local-marker'\n"
        "    # worker-source-marker\n"
        "    raise ValueError('remote failure')\n"
    )
    worker_path = tmp_path / "worker_task.py"
    worker_path.write_text("\n" * 120 + source, encoding="utf-8")
    result = dask_client.submit(
        _run_task_on_worker, str(worker_path), delete_source=delete_source
    ).result(timeout=45)
    worker_path.write_text(
        "\n" * 120 + source.replace("worker-source-marker", "wrong-checkout-marker"),
        encoding="utf-8",
    )
    task = TaskWithoutPath(name="fail", function=object)

    report = _create_failure_report(task, result)
    rendered = _render(report)

    assert isinstance(report, RemoteExecutionReport)
    assert f"{worker_path}:124 in fail" in rendered
    assert "ValueError: remote failure" in rendered
    assert "remote-local-marker" in rendered
    assert ("worker-source-marker" in rendered) is not delete_source
    assert "wrong-checkout-marker" not in rendered


@pytest.mark.parametrize(
    "remote_traceback",
    [
        RemoteTraceback(b"invalid trace data", {}),
        RemoteTraceback(b"invalid trace data", {}, rich_version="different"),
    ],
)
def test_remote_traceback_falls_back_when_trace_cannot_be_decoded(remote_traceback):
    task = TaskWithoutPath(name="fail", function=object)
    result = WrapperResult(
        carry_over_products=None,
        warning_reports=[],
        exc_info=(ValueError, ValueError("remote failure"), "worker fallback"),
        stdout="",
        stderr="",
        remote_traceback=remote_traceback,
    )

    report = _create_failure_report(task, result)

    assert type(report) is ExecutionReport
    assert "worker fallback" in _render(report)
