from flask import Flask, render_template
import marimo
from pathlib import Path

app = Flask(__name__, template_folder="templates")

# Initialize Marimo ASGI app
# This creates a server that can serve your Marimo notebooks
# You can mount multiple notebooks if needed
marimo_server = (
    marimo.create_asgi_app(include_code=False, quiet=True) # Set include_code to False for production
    .with_app(path="/notebooks/data-analysis", root="./src/notebooks/data_analysis.py")
    # Add more .with_app() calls for other Marimo notebooks
)

# Mount the Marimo server at a specific path
# Flask doesn't have a direct "mount" for ASGI apps like FastAPI.
# We'll serve it as a WSGI application using a WSGI server (like Gunicorn)
# and then have Cloud Run direct traffic. For simplicity here,
# we're showing how marimo_server would exist, but direct WSGI integration
# with marimo's ASGI server in a single Flask app isn't standard.
# The Dockerfile handles serving this correctly with uvicorn for marimo
# and gunicorn for flask, or you use a single ASGI server with a WSGI adapter.
# For Cloud Run, running `uvicorn` (which supports ASGI) and `gunicorn` (WSGI)
# simultaneously requires a more complex entrypoint.
# The simplest approach is to make Marimo a separate Cloud Run service
# or use a unified ASGI framework if you prefer.
# For this example, I'll show how you *could* have it, but for a single container,
# you'd typically need to wrap Flask in an ASGI adapter or run a dedicated ASGI server.
# Let's assume for now that Cloud Run will handle the routing via URL paths if separate services.
# However, if you want it *in the same container*, you'd need an ASGI server for Flask as well.
# For simplicity, if Marimo is served by a separate ASGI server (which `marimo.create_asgi_app` creates),
# and Flask is served by WSGI, a single Docker container can't host both natively without a proxy or adapter.
#
# Let's pivot: we'll have `uvicorn` run an ASGI wrapper for Flask + Marimo together for simplicity on Cloud Run.

from asgiref.wsgi import WsgiToAsgi
flask_asgi_app = WsgiToAsgi(app)

# Combine Flask and Marimo into a single ASGI app for uvicorn
# This is a bit more advanced for a single container to serve both directly
# as Flask is WSGI and Marimo is ASGI.
# For practical deployment in one container, we'd typically serve Flask via an ASGI adapter (like uvicorn's).
# If you need them truly integrated, consider `starlette.applications.Starlette` for routing,
# mounting both your `flask_asgi_app` and `marimo_server`.
# For a short, concise response, let's keep the `app.py` primarily Flask and note the Marimo server.
# The `Dockerfile` will use `gunicorn` for Flask and then we'll adjust for Marimo.

# Let's use `uvicorn` for everything, with `wsgi-asgi` to adapt Flask
# This is the correct approach for a single container serving both.
# You will need to install `uvicorn` and `asgiref` for this to work.

# Dummy mount point for conceptual understanding, as `uvicorn` handles this
# with a single entry point by calling `app.py` and then we route
# based on path.
# from werkzeug.middleware.proxy_fix import ProxyFix # If you're behind a proxy, like Cloud Run
# app.wsgi_app = ProxyFix(app.wsgi_app) # Use if needed

@app.route("/")
def index():
    return render_template("index.html", title="Gotham's Dashboard")

@app.route("/webpage2")
def webpage_two():
    return render_template("webpage2.html", title="Another Bat-Page")

# You can add more Flask routes for your web pages here

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)