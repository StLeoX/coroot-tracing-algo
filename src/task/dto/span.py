from datetime import timedelta, datetime

from src.config import timestamp_format


class Span:
    '''
    有关时间单位的统一：数据库存的是9位时间，内存存的是6位时间（Python仅支持6位，也就是 microseconds/μs/us）。
    '''

    def __init__(
            self,
            trace_id,
            span_id,
            timestamp_us,
            duration_us,
            caller,
            callee,
            container_id,
    ):
        self.span_id = span_id
        self.trace_id = trace_id
        self.parent_span_id = ''

        if type(timestamp_us) == str:
            timestamp_us = datetime.strptime(timestamp_us, timestamp_format)
        self.start_time = timestamp_us  # microseconds
        self.duration = duration_us  # microseconds
        self.end_time = self.start_time + timedelta(microseconds=self.duration)
        self.caller = caller  # network IP
        self.callee = callee  # network IP
        self.container_id = container_id  # 全局唯一的 container_id，类似于 process_id。
        # self.span_kind = span_kind  # coroot's span always comes from client-side

        self.child_spans = []  # 暂时无用。完全使用DB中的ParentSpanId。
        self.references = ()  # 暂时无用，类似于节点的边？

        self.tgid_write = 0
        self.tgid_read = 0

    def set_tgid(self, tgid_write, tgid_read):
        self.tgid_write = tgid_write
        self.tgid_read = tgid_read

    def set_parent_span_id(self, parent_span_id):
        self.parent_span_id = parent_span_id


# server-side event, like span
class SSE:
    def __init__(self, timestamp_us, duration_us, tgid_read, tgid_write):
        if type(timestamp_us) == str:
            timestamp_us = datetime.strptime(timestamp_us, timestamp_format)
        self.timestamp = timestamp_us  # microseconds
        self.duration = duration_us  # microseconds
        self.tgid_read = tgid_read
        self.tgid_write = tgid_write
