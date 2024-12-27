#!/bin/bash
./venv/bin/prefect server start &
sleep 15
./venv/bin/python -m src.main
