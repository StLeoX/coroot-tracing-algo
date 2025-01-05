#!/bin/bash
./venv/bin/prefect server start &
sleep 10 # wait for server ready
./venv/bin/python -m src.main
