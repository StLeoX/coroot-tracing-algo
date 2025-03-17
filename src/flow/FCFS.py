from datetime import datetime, timedelta

from prefect import flow

from src.config import *
from src.task.fcfs_tracing import update_children
from src.task.fetch_traces import fetch_spans
from src.task.update_trace_id import update_trace_ids


@flow(name="FCFS")
def FCFS():
    # 获取处理时间
    util_sec = datetime.utcnow() - timedelta(seconds=monitoring_delay_sec)
    since_sec = util_sec - timedelta(seconds=fetch_timeout_sec)
    # 拉取数据到内存
    fetch_1 = fetch_spans.submit(util_sec, since_sec)
    # 更新 parent 属性
    update_1 = update_children.submit(time_batch_spans=fetch_1.result(), wait_for=[fetch_1])
    # 更新 trace_id 属性
    update_2 = update_trace_ids.submit(time_batch_spans=fetch_1.result(), wait_for=[update_1])
    update_2.wait()
