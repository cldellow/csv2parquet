#!/bin/bash
set -euo pipefail

here="${BASH_SOURCE[0]}"
here=$(dirname "$here")
cd "$here"
pipenv run python -m csv2parquet "$@"
