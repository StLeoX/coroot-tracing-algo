import pandas
from prefect import task, get_run_logger

from src.task.init_variables import *

# 该 task 目前已弃用
@task(retries=3, retry_delay_seconds=2)
def fetch_services():
    # fixme 需要系统中所有的进程，哪怕是redis这样没有下游服务的进程。而 redis 进程是不会反映在 ContainerID 当中的。
    # fixme 目前使用 caller ip，并且不计入 edge service。
    # todo 然后还需要 limit 限制一下。还是 Clickhouse 有维护元数据？
    fetch_sql = f"SELECT DISTINCT SpanAttributes[\'net.host.name\'] AS HostIP FROM {t_trace}"
    spans_df = pandas.read_sql_query(fetch_sql, ch_engine)
    service_names = spans_df['HostIP'].tolist()
    get_run_logger().info(f"Service IP list: {service_names}")
    return service_names
