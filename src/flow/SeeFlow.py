"""
整合相关 task。
"""

from prefect import flow

from src.task.fetch_traces import fetch_spans
from src.task.seeflow_tracing import update_children
from src.task.update_trace_id import update_trace_ids


@flow(name="SeeFlow")
def SeeFlow():
    # 拉取数据到内存
    fetch_f = fetch_spans.submit()
    # 更新 parent 属性
    update1_f = update_children.submit(time_batch_spans=fetch_f.result(), wait_for=[fetch_f])
    # 更新 trace_id 属性
    update2_f = update_trace_ids.submit(time_batch_spans=fetch_f.result(), wait_for=[fetch_f, update1_f])
    update2_f.wait()
