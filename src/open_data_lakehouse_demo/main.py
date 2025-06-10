import os
from flask import Flask, render_template


templates_dir = os.path.join(os.path.dirname(__file__), "templates")

app = Flask(__name__, template_folder=templates_dir)

@app.route("/")
@app.route("/index")
def index():
	return render_template("index.html")

if __name__ == '__main__':
	app.run(debug=True, host="0.0.0.0", port=8080)