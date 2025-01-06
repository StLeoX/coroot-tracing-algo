#!/bin/bash
export COROOT_CLICKHOUSE_ADDRESS='127.0.0.1:8123'
export COROOT_TRACING_INTERVAL=5
export COROOT_TRACING_DELAY=5
export COROOT_TRACING_DEBUG=1

./venv/bin/prefect server stop
./venv/bin/prefect server start &
sleep 10 # wait for server ready
./venv/bin/python -m src.main
