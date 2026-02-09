# ---------------------------------------------------------------------------
# Legal Scraper — Docker image
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS base

# Prevent Python from writing .pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first (layer cache optimisation)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Default data directory (mount a volume for persistence)
ENV DATA_DIR=/app/data
VOLUME ["/app/data"]

ENTRYPOINT ["python", "cli.py"]
CMD ["status"]
