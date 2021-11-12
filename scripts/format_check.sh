#!/bin/bash

set -e

isort --check --diff .
black --check --diff .
flake8 --extend-exclude .venv .
