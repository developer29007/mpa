FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pdm

# Copy dependency files first so Docker can cache this layer independently of source changes
COPY pyproject.toml pdm.lock README.md ./

# Export prod deps from the lockfile and install with pip (no venv needed in containers)
RUN pdm export -o /tmp/requirements.txt --no-hashes \
    && pip install --no-cache-dir -r /tmp/requirements.txt

COPY src/ ./src/
COPY scripts/ ./scripts/
RUN chmod +x scripts/*.sh

# Both paths are needed: /app for `from src.api.*` imports, /app/src for `from itch.*` / `from consumers.*`
ENV PYTHONPATH=/app:/app/src

# Default: API server. Each Railway service overrides this via its own start command.
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8001"]
