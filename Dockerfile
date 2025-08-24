# ---- base runtime ----
FROM python:3.11-slim

# System deps (slim, but enough for our libs)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata \
 && rm -rf /var/lib/apt/lists/*

# Workdir
WORKDIR /app

# Install Python deps directly (no requirements/pyproject needed)
# Pin to sane, recent versions known to work well together.
RUN pip install --no-cache-dir \
    confluent-kafka==2.4.0 \
    SQLAlchemy==2.0.31 \
    psycopg2-binary==2.9.9 \
    pydantic==2.7.1 \
    tenacity==8.3.0 \
    python-dotenv==1.0.1

# Copy your source tree
# Expected structure:
# /app/app/main.py, consumer.py, dispatcher.py, db.py, config.py, schemas.py, ...
COPY app /app/app

# Non-root user
RUN useradd -m runner
USER runner

# Useful defaults
ENV PYTHONUNBUFFERED=1

# Start the service
CMD ["python", "-m", "app.main"]
