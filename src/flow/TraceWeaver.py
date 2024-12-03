"""
整合相关 task。
"""

from prefect import Flow

from src.task.fetch_spans import fetch_spans

with Flow("TraceWeaver") as TraceWeaver:
    #
    fetch_f = fetch_spans.submit()
    fetch_f.wait()
    # todo
