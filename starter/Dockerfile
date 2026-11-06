# CPython 3.14.5+ (3.14.0-3.14.4 are avoided). uv resolves from uv.lock only (--locked).
FROM python:3.14.5-slim-trixie AS build
COPY --from=ghcr.io/astral-sh/uv:0.13 /uv /usr/local/bin/uv
# git: ranvier is installed from a git tag (see [tool.uv.sources] in pyproject.toml).
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
ENV UV_PYTHON_DOWNLOADS=never UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1
WORKDIR /app
# Dependencies first: this layer is cached until pyproject.toml or uv.lock change.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY src ./src
COPY ranvier.toml ./
RUN uv sync --locked --no-dev

FROM python:3.14.5-slim-trixie
RUN useradd --system --create-home --uid 10001 app
WORKDIR /app
COPY --from=build --chown=app:app /app /app
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
USER app
EXPOSE 8000
# One image, three commands: `serve` (default), `worker`, `migrate` (see docker-compose.yml).
ENTRYPOINT ["ranvier"]
CMD ["serve"]
