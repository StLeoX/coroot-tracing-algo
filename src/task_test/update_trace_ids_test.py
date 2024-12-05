import unittest

from prefect.logging import disable_run_logger

from src.task.dto.span import Span
from src.task_test.utils import *


class Test_update_trace_ids(unittest.TestCase):
    def setUp(self):
        setup_test_database()

    def tearDown(self) -> None:
        truncate_tables_in_test_database()

    def test_case_hit_cache(self):
        foo_span = Span('',
                        '0123456789abcdef',
                        '2024-11-11 11:00:05.123456',
                        1_000_000,
                        '172.20.0.1',
                        '172.20.0.2',
                        '/docker/foo-svc-1')

        # bar is-parent-of foo
        bar_span = Span('',
                        '1123456789abcdef',
                        '2024-11-11 11:00:00.123456',
                        9_000_000,
                        '172.20.0.10',
                        '172.20.0.1',
                        '/docker/bar-svc-1')
        foo_span.set_parent_span_id(bar_span.span_id)

        # full cache
        batch_1 = {bar_span.span_id: bar_span,
                   foo_span.span_id: foo_span}

        insert_spans_into_test_database([foo_span, bar_span])

        switch_to_test_database()

        from src.task.update_trace_ids import update_trace_ids
        with disable_run_logger():
            update_trace_ids.fn(batch_1)

        resync_tables_in_test_database()

        # oracles
        self.assertEqual(bar_span.span_id, query_trace_id_from_test_database(foo_span.span_id))
        self.assertEqual(bar_span.span_id, query_trace_id_from_test_database(bar_span.span_id))

    # todo failed, because 'foo' doesn't exist in cache
    def test_case_miss_cache_1(self):
        foo_span = Span('',
                        '0123456789abcdef',
                        '2024-11-11 11:00:05.123456',
                        1_000_000,
                        '172.20.0.1',
                        '172.20.0.2',
                        '/docker/foo-svc-1')

        # bar is-parent-of foo
        bar_span = Span('',
                        '1123456789abcdef',
                        '2024-11-11 11:00:00.123456',
                        9_000_000,
                        '172.20.0.10',
                        '172.20.0.1',
                        '/docker/bar-svc-1')
        foo_span.set_parent_span_id(bar_span.span_id)

        batch_1 = {bar_span.span_id: bar_span}

        insert_spans_into_test_database([foo_span, bar_span])

        switch_to_test_database()

        from src.task.update_trace_ids import update_trace_ids
        with disable_run_logger():
            update_trace_ids.fn(batch_1)

        resync_tables_in_test_database()

        # oracles
        self.assertEqual(bar_span.span_id, query_trace_id_from_test_database(foo_span.span_id))
        self.assertEqual(bar_span.span_id, query_trace_id_from_test_database(bar_span.span_id))
        # fixme Actual == '', twice

    def test_case_miss_cache_2(self):
        foo_span = Span('',
                        '0123456789abcdef',
                        '2024-11-11 11:00:05.123456',
                        1_000_000,
                        '172.20.0.1',
                        '172.20.0.2',
                        '/docker/foo-svc-1')

        # bar is-parent-of foo
        bar_span = Span('',
                        '1123456789abcdef',
                        '2024-11-11 11:00:00.123456',
                        9_000_000,
                        '172.20.0.10',
                        '172.20.0.1',
                        '/docker/bar-svc-1')
        foo_span.set_parent_span_id(bar_span.span_id)

        batch_1 = {foo_span.span_id: foo_span}

        insert_spans_into_test_database([foo_span, bar_span])

        switch_to_test_database()

        from src.task.update_trace_ids import update_trace_ids
        with disable_run_logger():
            update_trace_ids.fn(batch_1)

        resync_tables_in_test_database()

        # oracles
        self.assertEqual(bar_span.span_id, query_trace_id_from_test_database(foo_span.span_id))
        self.assertEqual(bar_span.span_id, query_trace_id_from_test_database(bar_span.span_id))
