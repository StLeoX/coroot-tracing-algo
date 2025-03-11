import pandas
from prefect import task

from src.task.init_variables import *


@task(retries=3, retry_delay_seconds=2)
def fetch_services():
    # fixme 需要系统中所有的进程，哪怕是redis这样没有下游服务的进程。而 redis 进程是不会反映在 ContainerID 当中的。
    # todo 然后还需要 limit 限制一下。还是 Clickhouse 有维护元数据？
    fetch_sql = f"SELECT DISTINCT ResourceAttributes[\'container.id\'] AS ContainerID FROM {t_trace}"
    spans_df = pandas.read_sql_query(fetch_sql, ch_engine)
    service_names = spans_df['ContainerID'].tolist()
    return service_names
