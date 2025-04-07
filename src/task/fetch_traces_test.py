import pickle
import unittest
from datetime import datetime, timedelta

from prefect.logging import disable_run_logger

from src.task.dto.span import Span
from src.task.utils import *


class Test_fetch_spans(unittest.TestCase):
    def setUp(self):
        setup_test_database()

    def tearDown(self) -> None:
        truncate_tables_in_test_database()

    def test_insert_then_fetch(self):
        insert_spans_into_test_database([
            Span('',
                 '0123456789abcdef',
                 '2024-11-11 11:00:01.123456',
                 1_000_000,
                 '172.20.0.1',
                 '172.20.0.2',
                 '/docker/foo-svc-1')
        ])

        # switching database
        import src.task.init_variables
        src.task.init_variables.t_trace = t_trace_test

        since_sec = datetime.strptime('2024-11-11 11:00:00.0', timestamp_format)
        util_sec = since_sec + timedelta(seconds=5)

        # import FUT
        from src.task.fetch_traces import fetch_spans
        with disable_run_logger():
            sid_span_map = fetch_spans.fn(util_sec, since_sec)

        # be sure it returns the map
        self.assertEqual(1, len(sid_span_map))
        self.assertEqual(1_000_000, sid_span_map['0123456789abcdef'].duration)

    def test_fetch_all_then_dump(self):
        since_sec = datetime.strptime('2025-01-10 11:55:30.0', timestamp_format_no_quote)
        util_sec = since_sec + timedelta(seconds=10)

        # import FUT
        from src.task.fetch_traces import fetch_spans
        with disable_run_logger():
            sid_span_map = fetch_spans.fn(util_sec, since_sec)

        # be sure it returns the map
        self.assertEqual(50, len(sid_span_map))

        # 注意：改了 Span 的类定义，要重新生成 pkl 数据包
        # print(sid_span_map.popitem()[1])

        with open("/root/Source/obser/coroot-tracing-algo/test/testdata/dataset1.pkl", "wb") as f:
            pickle.dump(sid_span_map, f)

    def test_fetch_services_names(self):
         # import FUT
        from src.task.fetch_services import fetch_services
        with disable_run_logger():
            services = fetch_services.fn()
        print(services)

    def test_load(self):
        with open("/root/Source/obser/coroot-tracing-algo/test/testdata/dataset1.pkl", "rb") as f:
            sid_span_map: dict = pickle.load(f)
            self.assertEqual(50, len(sid_span_map))
            # print(sid_span_map.popitem()[1])
