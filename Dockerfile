FROM python:3.12-slim AS base

# uv: dep + venv manager. Pin the binary so reproducible builds
# don't drift.
COPY --from=ghcr.io/astral-sh/uv:0.5.18 /uv /uvx /bin/

WORKDIR /app

# Layer deps separately from source so the wheel cache survives
# code edits.
COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-install-project --no-dev || \
    uv sync --no-install-project --no-dev

COPY src ./src
COPY alembic ./alembic
COPY alembic.ini ./
RUN uv sync --frozen --no-dev || uv sync --no-dev

ENV PYTHONUNBUFFERED=1
EXPOSE 8004

CMD ["uv", "run", "uvicorn", "multichannel.main:app", \
     "--host", "0.0.0.0", "--port", "8004"]
