PYTHON ?= python
QUEUEWORKS ?= queueworks

.PHONY: install fixtures serve worker demo benchmark test lint format check

install:
	$(PYTHON) -m pip install -c requirements-lock.txt -e '.[dev]'

fixtures:
	$(QUEUEWORKS) fixtures

serve:
	$(QUEUEWORKS) serve --host 127.0.0.1 --port 8000

worker:
	$(QUEUEWORKS) worker --url http://127.0.0.1:8000

demo:
	$(QUEUEWORKS) demo --workers 3 --jobs 12

benchmark:
	$(QUEUEWORKS) benchmark --workload realistic --jobs 24 --repetitions 2 --warmup 4 --output-dir benchmarks/runs/realistic
	$(QUEUEWORKS) benchmark --workload synthetic --jobs 24 --repetitions 2 --warmup 4 --duration-ms 100 --iterations 0 --output-dir benchmarks/runs/synthetic

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

format:
	$(PYTHON) -m ruff format .

check: lint test
