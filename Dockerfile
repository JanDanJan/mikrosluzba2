FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tzdata \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /app

# deps (same set we used earlier)
RUN pip install --no-cache-dir \
    confluent-kafka==2.4.0 \
    SQLAlchemy==2.0.31 \
    psycopg2-binary==2.9.9 \
    pydantic==2.7.1 \
    tenacity==8.3.0 \
    python-dotenv==1.0.1

# bring in your repo
COPY . /app

ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app

# important: leave a generic entrypoint
ENTRYPOINT ["python"]
# default; we'll override this in compose:
CMD ["/app/app/main.py"]
