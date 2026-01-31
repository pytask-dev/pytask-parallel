"""Demo script for pending/running task status updates."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def _write_tasks(path: Path, n_tasks: int, sleep_s: float, jitter_s: float) -> None:
    task_file = path / "task_status_demo.py"
    lines = [
        "from __future__ import annotations",
        "",
        "from pathlib import Path",
        "import time",
        "",
        "from pytask import task",
        "",
        f"N_TASKS = {n_tasks}",
        f"SLEEP_S = {sleep_s}",
        f"JITTER_S = {jitter_s}",
        "",
        "for i in range(N_TASKS):",
        "    @task(id=str(i), kwargs={'produces': Path(f'out_{i}.txt')})",
        "    def task_sleep(produces, i=i):",
        "        time.sleep(SLEEP_S + (i % 3) * JITTER_S)",
        "        produces.write_text('done')",
        "",
    ]
    task_file.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a pytask demo to observe pending/running status updates."
    )
    parser.add_argument("--n-tasks", type=int, default=30)
    parser.add_argument("--sleep", type=float, default=2.0)
    parser.add_argument("--jitter", type=float, default=0.5)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument(
        "--backend",
        choices=["processes", "threads", "loky", "dask", "none"],
        default="processes",
    )
    parser.add_argument("--entries", type=int, default=30)
    parser.add_argument(
        "--live",
        action="store_true",
        help="Use the live rich table instead of raw logs.",
    )
    parser.add_argument(
        "--log-status",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Emit pending/running status logs from the main process.",
    )
    parser.add_argument(
        "--dir",
        type=Path,
        default=Path(__file__).with_name("pending_status_demo"),
        help="Directory to store the demo task file.",
    )
    args = parser.parse_args()

    demo_dir = args.dir.resolve()
    demo_dir.mkdir(parents=True, exist_ok=True)

    for path in demo_dir.glob("out_*.txt"):
        path.unlink()

    _write_tasks(demo_dir, args.n_tasks, args.sleep, args.jitter)

    cmd = [
        sys.executable,
        "-m",
        "pytask",
        demo_dir.as_posix(),
        "--n-workers",
        str(args.workers),
        "--parallel-backend",
        args.backend,
        "--n-entries-in-table",
        str(args.entries),
    ]
    if not args.live:
        cmd.extend(["-s", "-v", "0"])
    sys.stdout.write(f"Running: {' '.join(cmd)}\n")
    sys.stdout.flush()
    env = dict(os.environ)
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")
    if args.log_status:
        env.setdefault("PYTASK_PARALLEL_DEBUG_STATUS", "1")
    return subprocess.call(cmd, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
