FROM python:3.12-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen --no-cache

# Copy code
COPY . .

# Expose port
EXPOSE 8080

# Set path to use venv
ENV PATH="/app/.venv/bin:$PATH"

# Run marimo server to allow selecting notebooks from the directory
CMD ["uv", "run", "marimo", "edit", "--host", "0.0.0.0", "--port", "8080", "--no-token", "notebooks"]
