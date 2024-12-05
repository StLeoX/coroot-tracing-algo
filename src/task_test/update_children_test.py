import unittest

from prefect.logging import disable_run_logger

from src.task.dto.span import Span, SSE
from src.task_test.utils import *


class Test_update_children(unittest.TestCase):
    def setUp(self):
        setup_test_database()

    def tearDown(self) -> None:
        truncate_tables_in_test_database()

    def test_case_foo_bar(self):
        foo_span = Span('',
                        '0123456789abcdef',
                        '2024-11-11 11:00:05.123456',
                        1_000_000,
                        '172.20.0.1',
                        '172.20.0.2',
                        '/docker/foo-svc-1')
        foo_span.set_tgid(100, 101)

        foo_sse = SSE('2024-11-11 11:00:04.123456',
                      3_000_000,
                      100,
                      101)

        # bar is-parent-of foo
        bar_span = Span('',
                        '1123456789abcdef',
                        '2024-11-11 11:00:00.123456',
                        9_000_000,
                        '172.20.0.10',
                        '172.20.0.1',
                        '/docker/bar-svc-1')

        batch_0 = {bar_span.span_id: bar_span}

        insert_spans_into_test_database([foo_span, bar_span])

        insert_sses_into_test_database([foo_sse])

        switch_to_test_database()

        from src.task.update_children import update_children
        with disable_run_logger():
            update_children.fn(batch_0)

        resync_tables_in_test_database()  # avoid data race

        # oracles
        self.assertEqual(bar_span.span_id, query_parent_span_id_from_test_database(foo_span.span_id))
        # self.assertEqual(bar_span.span_id, batch_0[foo_span.span_id].parent_span_id) # fixme map KeyError
