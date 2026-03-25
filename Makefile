.PHONY: setup install run run-real flow test clean docker-build docker-run

setup:
	python -m venv .venv
	. .venv/bin/activate && pip install -r requirements.txt && pip install -e .

install:
	pip install -r requirements.txt && pip install -e .

run:
	. .venv/bin/activate && python -m src.pipeline.run_pipeline --source synthetic --start-date 2025-12-01 --days 7 --rows-per-day 5000

run-real:
	. .venv/bin/activate && python -m src.pipeline.run_pipeline --source nyc_tlc --tlc-dataset yellow --tlc-year 2025 --tlc-month 1 --run-id nyc-tlc-demo

flow:
	. .venv/bin/activate && python -m src.pipeline.prefect_flow --source synthetic --start-date 2025-12-01 --days 1 --rows-per-day 100 --run-id prefect-local

test:
	. .venv/bin/activate && pytest -q

clean:
	rm -rf data .pytest_cache

docker-build:
	docker build -t city-rides-lakehouse .

docker-run:
	docker run --rm -v "$(PWD)/data:/app/data" city-rides-lakehouse
