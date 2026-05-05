.PHONY: install run test results clean

install:
	pip install hl7apy fhir.resources pytest

run:
	python run_pipeline.py

test:
	python -m pytest tests/ -v

results:
	python run_pipeline.py

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete 2>/dev/null; true
	find . -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null; true
	find . -name ".coverage" -delete 2>/dev/null; true
