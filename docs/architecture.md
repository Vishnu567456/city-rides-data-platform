# Architecture Overview

This project implements a lakehouse-style pipeline using local storage and DuckDB as a stand-in for a cloud data warehouse. The design separates raw, cleaned, and aggregated datasets, preserves append-only raw files, records operational audit metadata for each run, and can ingest either generated trips or real NYC TLC trip data.

## Data Flow
- Generate synthetic trip data or ingest NYC TLC parquet files
- Write raw Parquet files to a partitioned bronze layer with `source_batch_id` and `ingested_at`
- Transform to silver and gold tables in DuckDB
- Deduplicate silver records on `trip_id`, keeping the latest ingested record
- Execute quality checks and emit a machine-readable report
- Persist run summaries to JSON and the `pipeline_runs` audit table
- Optionally orchestrate the whole run through Prefect

## Storage Layers
- Bronze: raw, append-only Parquet files partitioned by pickup date
- Silver: cleaned, typed, deduplicated trips with ingestion metadata
- Gold: hourly metrics for analytics
- Monitoring: JSON run summaries plus DuckDB audit records in `pipeline_runs`

## Orchestration
The pipeline is runnable as a single command or through a Prefect flow. Each run can be traced by `run_id`, which makes it easier to add external orchestration later without changing the warehouse model.
