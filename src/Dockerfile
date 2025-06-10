# Dockerfile
FROM python:3.12-slim

# Install uv (as it's not pre-installed in Python images)
RUN pip install uv

WORKDIR /app/src

# Copy pyproject.toml and install dependencies using uv
COPY pyproject.toml .
# `uv sync` installs all direct and transitive dependencies
RUN uv sync

# Copy your application source code
COPY ./ /app/src

ENV PORT 8080
EXPOSE $PORT

# Command to run the ASGI application using uvicorn
# The `app` object from src/combined-with-marimo-main.py will be served.
CMD ["uv", "run", "/app/src/open_data_lakehouse_demo/main.py"]