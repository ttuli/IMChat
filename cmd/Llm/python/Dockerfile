# Use a Python image with uv pre-installed
FROM astral-sh/uv:python3.12-alpine AS builder

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a separate volume
ENV UV_LINK_MODE=copy

# Install the project's dependencies
WORKDIR /app

# Copy dependency files from the specific internal app directory
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=internal/apps/Llm/python/uv.lock,target=uv.lock \
    --mount=type=bind,source=internal/apps/Llm/python/pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

# Then, install the rest of the project
ADD . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Final image
FROM python:3.12-alpine

WORKDIR /app

# Copy the build artifacts
COPY --from=builder /app /app

# Place executables in the path
ENV PATH="/app/.venv/bin:$PATH"

# Set PYTHONPATH to include the root so internal imports work
ENV PYTHONPATH="/app"

# Run the entry point from cmd/
CMD ["python", "cmd/Llm/python/main.py"]
