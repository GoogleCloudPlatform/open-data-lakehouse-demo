# Use an official Python runtime as a parent image
FROM python:3.14-slim

# Copy uv binary from the official uv image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set working directory inside the container
WORKDIR /app

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app" \
    PORT=8080 \
    UV_DEFAULT_INDEX="https://pypi.org/simple" \
    UV_INDEX_URL="https://pypi.org/simple"

# Copy dependency files first for layer caching
COPY pyproject.toml uv.lock ./

# Install project dependencies using uv from public PyPI (without --frozen to allow overriding workstation artifact registry URLs in uv.lock)
RUN uv sync --no-cache --no-install-project --default-index="https://pypi.org/simple" --index-url="https://pypi.org/simple"

# Copy the rest of the application files
COPY . .

# Install the project itself
RUN uv sync --no-cache --default-index="https://pypi.org/simple" --index-url="https://pypi.org/simple"

# Place virtualenv executables in PATH
ENV PATH="/app/.venv/bin:$PATH"

# Expose port 8080 for the application
EXPOSE 8080

# Run Streamlit application
CMD ["streamlit", "run", "st_demo/app.py", "--server.port=8080", "--server.address=0.0.0.0", "--server.headless=true"]
