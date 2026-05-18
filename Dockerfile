FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:${PATH}"

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY main.py config.shared.toml ./
COPY assets ./assets

RUN uv sync --frozen --no-dev \
    && useradd --create-home --shell /usr/sbin/nologin eatbot \
    && mkdir -p /app/logs \
    && chown -R eatbot:eatbot /app

USER eatbot

CMD ["eatbot", "run"]
