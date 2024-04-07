unittest:
	python -m pytest -s tests/unit

integration:
	python -m pytest -s tests/integration

testsuite:
    python -m pytest -n4

install:
	pip install -e ".[dev]"
