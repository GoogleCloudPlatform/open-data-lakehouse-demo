# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Stage 1: The Builder
# This stage installs dependencies, downloads the Tailwind CLI, and builds the CSS.
FROM python:3.11-slim AS builder

# Install uv and curl
RUN pip install uv
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Download the standalone Tailwind CLI
RUN curl -sLO https://github.com/tailwindlabs/tailwindcss/releases/latest/download/tailwindcss-linux-x64 \
    && chmod +x tailwindcss-linux-x64 \
    && mv tailwindcss-linux-x64 /usr/local/bin/tailwindcss

# Copy all application and static source files
COPY . .

# Build the production CSS file
# This generates the final static/dist/output.css
RUN tailwindcss -i ./open_data_lakehouse_demo/static/src/input.css -o ./open_data_lakehouse_demo/static/css/style.css --minify


# Dockerfile
FROM python:3.11-slim
RUN apt update -y

# Install uv (as it's not pre-installed in Python images)
RUN pip install uv

WORKDIR /app/src

# Copy pyproject.toml and install dependencies using uv
COPY pyproject.toml .
# `uv sync` installs all direct and transitive dependencies
RUN uv sync
RUN uv pip install -e .


# Copy your application source code
COPY ./ /app/src

## Copy the built CSS and the rest of the application from the builder stage
COPY --from=builder  /app/open_data_lakehouse_demo/static/css /app/src/open_data_lakehouse_demo/static/css
COPY --from=builder  /app/open_data_lakehouse_demo/templates /app/src/open_data_lakehouse_demo/templates

ENV PORT 8080
EXPOSE $PORT

CMD ["uv", "run", "/app/src/open_data_lakehouse_demo/main.py"]
