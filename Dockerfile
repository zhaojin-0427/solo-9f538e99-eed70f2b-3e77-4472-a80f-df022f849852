FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shard_planner ./shard_planner
COPY wsgi.py .

RUN useradd --system --uid 10001 appuser \
    && mkdir -p /data \
    && chown appuser:appuser /data
USER appuser

ENV PLANNER_DB=/data/plans.db

EXPOSE 8000

CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "2", "--threads", "4", "--access-logfile", "-", "wsgi:app"]
