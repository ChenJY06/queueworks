"""Command-line entry points for the coordinator, workers, and experiments."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import httpx

from queueworks.handlers import generate_fixtures
from queueworks.worker import Worker


def _data_dir(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--data-dir", type=Path, default=Path(os.getenv("QUEUEWORKS_DATA_DIR", "data"))
    )


def _url(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--url", default="http://127.0.0.1:8000")


def _positive_int(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(prog="queueworks", description=__doc__)
    commands = command.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="start the single-process HTTP coordinator")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=_positive_int, default=8000)

    worker = commands.add_parser("worker", help="start an independent HTTP worker")
    _url(worker)
    _data_dir(worker)
    worker.add_argument("--worker-id")
    worker.add_argument("--poll-interval", type=float, default=0.1)
    worker.add_argument("--shutdown-timeout", type=float, default=5)
    worker.add_argument("--worker-key", default=os.getenv("QUEUEWORKS_WORKER_KEY"))

    fixtures = commands.add_parser("fixtures", help="generate the deterministic demo data")
    _data_dir(fixtures)

    submit = commands.add_parser("submit", help="submit a supported job")
    _url(submit)
    submit.add_argument("--kind", choices=("checksum", "thumbnail", "synthetic"), required=True)
    submit.add_argument("--payload", type=json.loads, required=True, help="a JSON object")
    submit.add_argument("--max-attempts", type=_positive_int, default=3)
    submit.add_argument("--idempotency-key")

    inspect = commands.add_parser("inspect", help="inspect a job and its attempts")
    _url(inspect)
    inspect.add_argument("id")

    listing = commands.add_parser("list", help="list a bounded page of jobs")
    _url(listing)
    listing.add_argument("--status", choices=("queued", "running", "succeeded", "failed"))
    listing.add_argument("--limit", type=_positive_int, default=20)
    listing.add_argument("--offset", type=int, default=0)

    demo = commands.add_parser("demo", help="run a self-cleaning worker crash demonstration")
    _data_dir(demo)
    demo.add_argument("--workers", type=_positive_int, default=3)
    demo.add_argument("--jobs", type=_positive_int, default=12)
    demo.add_argument("--output-dir", type=Path, default=Path("artifacts/demo"))
    demo.add_argument("--lease-seconds", type=float, default=1.5)

    benchmark = commands.add_parser("benchmark", help="measure 1, 2, and 4 workers")
    _data_dir(benchmark)
    benchmark.add_argument("--workload", choices=("realistic", "synthetic", "all"), default="all")
    benchmark.add_argument("--workers", type=_positive_int, nargs="+", default=[1, 2, 4])
    benchmark.add_argument("--jobs", type=_positive_int, default=24)
    benchmark.add_argument("--repetitions", type=_positive_int, default=2)
    benchmark.add_argument("--warmup", type=int, default=4)
    benchmark.add_argument("--duration-ms", type=int, default=100)
    benchmark.add_argument("--iterations", type=int, default=0)
    benchmark.add_argument("--lease-seconds", type=float, default=1.5)
    benchmark.add_argument("--output-dir", type=Path, default=Path("benchmarks/runs"))
    return command


def _print(value: Any) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    try:
        if arguments.command == "serve":
            import uvicorn

            uvicorn.run(
                "queueworks.api:create_app",
                factory=True,
                host=arguments.host,
                port=arguments.port,
                workers=1,
            )
        elif arguments.command == "worker":
            Worker(
                arguments.url,
                arguments.data_dir,
                worker_id=arguments.worker_id,
                poll_interval=arguments.poll_interval,
                shutdown_timeout=arguments.shutdown_timeout,
                worker_key=arguments.worker_key,
            ).run()
        elif arguments.command == "fixtures":
            _print(generate_fixtures(arguments.data_dir))
        elif arguments.command in ("demo", "benchmark"):
            from queueworks.experiments import run_benchmark, run_demo

            # The module accepts a namespace to keep the experiment options
            # visible and readable without a second configuration abstraction.
            arguments.output = arguments.output_dir
            action = run_demo if arguments.command == "demo" else run_benchmark
            action(arguments)
        else:
            with httpx.Client(base_url=arguments.url, timeout=10, trust_env=False) as client:
                if arguments.command == "submit":
                    headers = (
                        {"Idempotency-Key": arguments.idempotency_key}
                        if arguments.idempotency_key
                        else {}
                    )
                    response = client.post(
                        "/jobs",
                        json={
                            "kind": arguments.kind,
                            "payload": arguments.payload,
                            "max_attempts": arguments.max_attempts,
                        },
                        headers=headers,
                    )
                elif arguments.command == "inspect":
                    response = client.get(f"/jobs/{arguments.id}")
                else:
                    parameters = {"limit": arguments.limit, "offset": arguments.offset}
                    if arguments.status:
                        parameters["status"] = arguments.status
                    response = client.get("/jobs", params=parameters)
                response.raise_for_status()
                _print(response.json())
    except httpx.HTTPStatusError as error:
        print(f"HTTP {error.response.status_code}: {error.response.text}")
        return 1
    except (httpx.RequestError, ValueError, OSError) as error:
        print(f"Error: {error}")
        return 1
    return 0
