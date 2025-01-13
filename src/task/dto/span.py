from datetime import timedelta


class Span:
    def __init__(
            self,
            trace_id,
            span_id,
            start_timestamp,
            duration,
            caller,
            callee,
            container_id,
    ):
        self.span_id: str = span_id
        self.trace_id: str = trace_id
        self.parent_span_id: str = ''

        self.start_time = start_timestamp  # microseconds, us, 微秒（6位）
        self.duration = duration  # microseconds
        self.end_time = start_timestamp + timedelta(milliseconds=self.duration)  # fixme
        self.caller = caller  # using network IP
        self.callee = callee  # using network IP
        self.container_id = container_id  # 全局唯一的 container_id，类似于 process_id。
        # self.span_kind = span_kind  # coroot's span always comes from client-side

        self.child_spans = []  # 暂时无用。完全使用DB中的ParentSpanId。
        self.references = ()  # 暂时无用，类似于节点的边？


class ChildCandidate:
    def __init__(self, span_id, timestamp, duration):
        self.span_id = span_id
        self.timestamp = timestamp  # microseconds
        self.duration = duration  # microseconds

    def get_timestamp(self) -> str:
        t = self.timestamp + timedelta(hours=8)
        return t.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # milliseconds

    def get_timestamp_plus_duration(self) -> str:
        t = self.timestamp + timedelta(hours=8)
        t += timedelta(microseconds=self.duration)  # fixme
        return t.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
