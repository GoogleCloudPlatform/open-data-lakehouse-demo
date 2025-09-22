# Dev Utils for the demo

This folder contains a couple of assets to help with local development.

- `clean-notebooks.py` is a utility to clean the outputs of the notebooks saved in the `notebooks_and_code` folder. just
  to make things cleaner to use in git. The supporting files `.python-version` and `pyproject.toml` are just for the
  `uv` environemnt to run the script, but use the script any way you want.
- `docker-compose.yaml` will setup a spark and kafka cluster to help with local development of the real-time features of
  this demo.