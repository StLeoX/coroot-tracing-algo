import unittest
from datetime import datetime, timedelta

from prefect.logging import disable_run_logger

from src.cache import new_span_cache
from src.task.dto.span import Span
from src.task_test.utils import *


class Test_fetch_spans(unittest.TestCase):
    def setUp(self):
        setup_test_database()

    def tearDown(self) -> None:
        truncate_tables_in_test_database()

    def test_case_foo(self):
        with disable_run_logger():
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
            span_cache_1 = new_span_cache()

            switch_to_test_database()

            # FUT
            from src.task.fetch_spans import fetch_spans_helper
            fetch_spans_helper(util_sec, since_sec, span_cache_1)

            # oracles
            self.assertEqual(1, len(span_cache_1))
            self.assertEqual(foo.span_id, span_cache_1[foo.span_id].span_id)

            time.sleep(cache_timeout_sec)  # trigger cache timeout
            self.assertEqual(0, len(span_cache_1))
