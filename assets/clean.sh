#!/bin/bash

uv run nbstripout **/*.ipynb
uv run jupytext --sync **/*.ipynb --to py:percent --pipe black
