import unittest

from prefect.logging import disable_run_logger
from prefect.states import StateType

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
            state = fetch_spans.fn(util_sec, since_sec)

        # oracles
        self.assertEqual(StateType.COMPLETED, state.type)
        from src.globals import span_cache
        self.assertEqual(1, len(span_cache))
        self.assertEqual(foo.span_id, span_cache[foo.span_id].span_id)

        time.sleep(cache_timeout_sec)
        self.assertEqual(0, len(span_cache))
