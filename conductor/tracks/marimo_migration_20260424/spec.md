# Specification - Migrate remaining Jupyter notebooks to Marimo notebooks

## Overview
This track focuses on completing the migration of data processing and analysis notebooks from the Jupyter format (.ipynb) to the Marimo format (.py). Marimo offers a reactive, pure-Python notebook experience that is better suited for version control and reproducible data science.

## User Stories
- As a Data Engineer, I want all my notebooks in Marimo format so that I can easily track changes in Git.
- As a Developer, I want to run notebooks as pure Python scripts to integrate them into CI/CD pipelines.

## Functional Requirements
- Convert all `.ipynb` files in `data-journey/notebooks/` to Marimo `.py` files.
- Ensure all logic, imports, and visualizations are preserved and functional in the Marimo environment.
- Verify that the Marimo notebooks can be executed as standalone scripts.

## Non-Functional Requirements
- Maintain existing logic and data processing results.
- Code must follow the project's Python style guide.
- High reliability in data loading and Spark processing steps.
