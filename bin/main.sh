#!/usr/bin/env bash

CONF=prod.json

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

source "${APP_DIR}/venv/bin/activate"
python3 "${APP_DIR}/src/main.py" --conf "${APP_DIR}/conf/${CONF}" --state-backend elastic
PROCESS_STATUS=$?
deactivate

exit $PROCESS_STATUS

