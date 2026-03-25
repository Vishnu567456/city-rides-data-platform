FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt pyproject.toml README.md ./
COPY src ./src
COPY app.py Makefile ./
COPY dbt ./dbt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -e .

CMD ["python", "-m", "src.pipeline.run_pipeline", "--source", "synthetic", "--start-date", "2025-12-01", "--days", "1", "--rows-per-day", "100"]
