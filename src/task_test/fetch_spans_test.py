import unittest
from datetime import datetime, timedelta

from prefect.logging import disable_run_logger

from src.task.dto.span import Span
from src.task_test.utils import *


class Test_fetch_spans(unittest.TestCase):
    def setUp(self):
        setup_test_database()

    def tearDown(self) -> None:
        truncate_tables_in_test_database()

    def test_case_foo(self):
        foo = Span('',
                   '0123456789abcdef',
                   '2024-11-11 11:00:01.123456',
                   1_000_000,
                   '172.20.0.1',
                   '172.20.0.2',
                   '/docker/foo-svc-1')
        insert_spans_into_test_database([foo])

        since_sec = datetime.strptime('2024-11-11 11:00:00.0', timestamp_format)
        util_sec = since_sec + timedelta(seconds=5)

        switch_to_test_database()

        from src.task.fetch_spans import fetch_spans
        with disable_run_logger():
            sid_span_map = fetch_spans.fn(util_sec, since_sec)

        # oracles
        self.assertEqual(1, len(sid_span_map))  # be sure it returns the map
        self.assertEqual(1_000_000, sid_span_map['0123456789abcdef'].duration)
