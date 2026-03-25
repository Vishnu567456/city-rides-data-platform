# Orchestration

This project includes a Prefect flow wrapper around the core pipeline.

## Local run
```bash
python -m src.pipeline.prefect_flow \
  --source synthetic \
  --start-date 2025-12-01 \
  --days 1 \
  --rows-per-day 100 \
  --run-id prefect-demo
```

## Real data run
```bash
python -m src.pipeline.prefect_flow \
  --source nyc_tlc \
  --tlc-dataset yellow \
  --tlc-year 2025 \
  --tlc-month 1 \
  --run-id prefect-nyc-tlc
```

## Why this matters
- Retries are configured at the task level.
- The flow reuses the same audit-aware `run_pipeline` function as the CLI.
- Moving this into Prefect Cloud later will not require a warehouse model rewrite.
