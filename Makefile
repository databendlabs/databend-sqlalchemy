unittest:
	python -m pytest -s tests/unit

integration:
	python -m pytest -s tests/integration

sampletest:
	python databend_sqlalchemy/test.py

install:
	pip install -e ".[dev]"
