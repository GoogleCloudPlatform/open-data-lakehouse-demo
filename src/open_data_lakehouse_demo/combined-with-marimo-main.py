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
# See the License for the specific language govemrning permissions and
# limitations under the License.

from flask import Flask, render_template, request, redirect, url_for, session
from werkzeug.security import check_password_hash, generate_password_hash
import marimo
import os
import logging
from dotenv import load_dotenv
from functools import wraps
from starlette.middleware.wsgi import WSGIMiddleware
from starlette.applications import Starlette
from starlette.routing import Mount, Route
from starlette.responses import RedirectResponse

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

notebooks_dir = os.path.join(os.path.dirname(__file__), "marimo-notebooks")
templates_dir = os.path.join(os.path.dirname(__file__), "templates")

app = Flask(__name__, template_folder=templates_dir)
# app.config['SECRET_KEY'] = os.getenv("SECRET_KEY", "your-secret-key")
# app.config['SESSION_TYPE'] = 'filesystem'
# Session(app)

app_names: list[str] = []

marimo_app = marimo.create_asgi_app()
for filename in sorted(os.listdir(notebooks_dir)):
    if filename.endswith(".py"):
        app_name = os.path.splitext(filename)[0].replace("_", "-")
        app_path = os.path.join(notebooks_dir, filename)
        logging.info(f"Adding app {app_name} from {app_path}")
        marimo_app = marimo_app.with_app(path=f"/{app_name}", root=app_path)
        app_names.append(app_name)

# Wrap the Flask app with WSGIMiddleware
wsgi_app = WSGIMiddleware(app)

@app.route("/")
def index():
    return render_template("index.html", title="Gotham's Dashboard", app_names=app_names)

@app.route("/webpage2")
def webpage_two():
    return render_template("webpage2.html", title="Another Bat-Page")

# --- Combine Flask (as ASGI) and Marimo with Starlette Router ---
# Starlette acts as the central ASGI application, routing requests.
# Flask WSGI app wrapped in WsgiToAsgi to make it an ASGI app.

# Create the final ASGI app
asgi_app = Starlette(routes=[
    Route("/", endpoint=lambda req: RedirectResponse(url="/web/")),
    Mount("/web", app=wsgi_app),
    Mount("/", app=marimo_app.build()),
])

if __name__ == '__main__':
    import uvicorn
    # print valid routes recursively
    for route in asgi_app.routes:
        logger.info(route)
    uvicorn.run(asgi_app, host="0.0.0.0", port=8080)