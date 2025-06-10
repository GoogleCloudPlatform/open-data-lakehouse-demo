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