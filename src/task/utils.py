'''
一些操作 Clickhouse 数据库的帮助函数。
'''

import pandas

from src.task.init_variables import *


def update_parent_mock(sid_span_map, child_span_id, parent_span_id):
    print(f"[deb] span_id triple (child - parent - gt_parent): "
          f"{child_span_id} - {parent_span_id} - {sid_span_map[child_span_id].gt_parent_span_id}")


def update_parent(sid_span_map, child_span_id, parent_span_id):
    # 先更新 map
    if child_span_id in sid_span_map:
        sid_span_map[child_span_id].parent_span_id = parent_span_id
    # 后更新 DB
    update_sql = f"ALTER TABLE {t_trace} " \
                 f"UPDATE ParentSpanId = \'{parent_span_id}\' " \
                 f"WHERE SpanId = \'{child_span_id}\';"
    try:
        pandas.read_sql_query(update_sql, ch_engine)
    except:
        print(f"Failed to update mapping: ({child_span_id}, {parent_span_id})")


'''
一些操作 Clickhouse 数据库的帮助函数。
同于测试环境
'''


def setup_test_database():
    pandas.read_sql_query(f"CREATE DATABASE IF NOT EXISTS `test`", ch_engine)
    # https://clickhouse.com/docs/en/sql-reference/statements/create/table#with-a-schema-and-data-cloned-from-another-table
    pandas.read_sql_query(f"CREATE TABLE IF NOT EXISTS {t_trace_test} AS {t_trace}", ch_engine)
    # pandas.read_sql_query(f"CREATE TABLE IF NOT EXISTS {t_l7ss_test} AS {t_l7ss}", ch_engine)


def truncate_tables_in_test_database():
    pandas.read_sql_query(f"TRUNCATE TABLE IF EXISTS {t_trace_test}", ch_engine)
    # pandas.read_sql_query(f"TRUNCATE TABLE IF EXISTS {t_l7ss_test}", ch_engine)


def insert_spans_into_test_database(spans):
    # https://clickhouse.com/docs/zh/sql-reference/data-types/map
    insert_sql = f"INSERT INTO {t_trace_test} (SpanId, Timestamp, Duration, ResourceAttributes, SpanAttributes) VALUES "
    for span in spans:
        insert_sql += f"('{span.span_id}', " \
                      f"'{span.start_time.strftime(timestamp_format)}', " \
                      f"{span.duration * 1000}, " \
                      f"{{'container.id': '{span.container_id}'}}, " \
                      f"{{'net.host.name': '{span.caller}', 'net.peer.name': '{span.callee}'}})"
    print('[deb]', insert_sql)
    pandas.read_sql_query(insert_sql, ch_engine)
