import time

import pandas

from src.globals import *


def setup_test_database():
    pandas.read_sql_query(f"CREATE DATABASE IF NOT EXISTS test", ch_engine)
    # https://clickhouse.com/docs/en/sql-reference/statements/create/table#with-a-schema-and-data-cloned-from-another-table
    pandas.read_sql_query(f"CREATE TABLE IF NOT EXISTS {t_trace_test} AS {t_trace}", ch_engine)
    pandas.read_sql_query(f"CREATE TABLE IF NOT EXISTS {t_l7ss_test} AS {t_l7ss}", ch_engine)


def truncate_tables_in_test_database():
    pandas.read_sql_query(f"TRUNCATE TABLE IF EXISTS {t_trace_test}", ch_engine)
    pandas.read_sql_query(f"TRUNCATE TABLE IF EXISTS {t_l7ss_test}", ch_engine)


def switch_to_test_database():
    import src.globals
    src.globals.t_trace = t_trace_test
    src.globals.t_l7ss = t_l7ss_test


def resync_tables_in_test_database():
    pandas.read_sql_query(f"OPTIMIZE TABLE {t_trace_test}", ch_engine)
    time.sleep(3)


def insert_spans_into_test_database(spans):
    # https://clickhouse.com/docs/zh/sql-reference/data-types/map
    insert_sql = f"INSERT INTO {t_trace_test} (SpanId, ParentSpanId, Timestamp, Duration, ResourceAttributes, SpanAttributes) VALUES "
    for span in spans:
        insert_sql += f"('{span.span_id}', " \
                      f"'{span.parent_span_id}', " \
                      f"'{span.start_time.strftime(timestamp_format)}', " \
                      f"{span.duration * 1000}, " \
                      f"{{'container.id': '{span.container_id}'}}, " \
                      f"{{'net.host.name': '{span.caller}', " \
                      f"  'net.peer.name': '{span.callee}', " \
                      f"  'tgid_req_cs': '{span.tgid_write}', " \
                      f"  'tgid_resp_cs': '{span.tgid_read}' }}), "
    pandas.read_sql_query(insert_sql, ch_engine)


def insert_sses_into_test_database(sses):
    insert_sql = f"INSERT INTO {t_l7ss_test} (Timestamp, Duration, TgidRead, TgidWrite) VALUES "
    for sse in sses:
        insert_sql += f"('{sse.timestamp.strftime(timestamp_format)}', " \
                      f"{sse.duration * 1000}, " \
                      f"{sse.tgid_read}, " \
                      f"{sse.tgid_write}), "
    pandas.read_sql_query(insert_sql, ch_engine)


def query_parent_span_id_from_test_database(span_id):
    query_sql = f"SELECT ParentSpanId FROM {t_trace_test} WHERE SpanId = '{span_id}'"
    result_df = pandas.read_sql_query(query_sql, ch_engine)
    if len(result_df) != 1:
        return ''
    return result_df['ParentSpanId'][0]


def query_trace_id_from_test_database(span_id):
    query_sql = f"SELECT TraceId FROM {t_trace_test} WHERE SpanId = '{span_id}'"
    result_df = pandas.read_sql_query(query_sql, ch_engine)
    if len(result_df) != 1:
        return ''
    return result_df['TraceId'][0]
