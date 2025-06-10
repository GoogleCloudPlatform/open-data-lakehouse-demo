import json
import os
import pathlib

import ruff


def clean_notebooks(notebook_path: str):
    with open(notebook_path, "r") as fp:
        notebook_json = json.load(fp)
    cells = notebook_json["cells"]
    for cell in cells:
        if cell["cell_type"] == "code":
            cell["execution_count"] = None
            cell["outputs"] = []
    notebook_json["cells"] = cells
    with open(notebook_path, "w") as fp:
        json.dump(notebook_json, fp)





if __name__ == "__main__":
    current_dir = pathlib.Path(os.getcwd())
    notebooks_dir = os.path.join(current_dir.parent.parent, "notebooks")
    for root, dirs, files in os.walk(notebooks_dir):
        for file in files:
            if file.endswith(".ipynb"):
                notebook_path = os.path.join(root, file)
                print(f"Cleaning outputs from {notebook_path}")
                clean_notebooks(notebook_path)