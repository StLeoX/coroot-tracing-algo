from datetime import timedelta


class Span:
    def __init__(
            self,
            trace_id,
            span_id,
            start_timestamp,  # nanoseconds
            duration,  # nanoseconds
            caller,
            callee,
            container_id,
    ):
        self.span_id: str = span_id
        self.trace_id: str = trace_id
        self.parent_span_id: str = ''

        self.start_time = start_timestamp  # milliseconds，pandas 只支持微秒（6位）
        self.duration = duration // 1000  # nanoseconds 转 milliseconds
        self.end_time = start_timestamp + timedelta(milliseconds=self.duration)
        self.caller = caller  # using network IP
        self.callee = callee  # using network IP
        self.container_id = container_id  # 全局唯一的 container_id，类似于 process_id。
        # self.span_kind = span_kind  # coroot's span always comes from client-side

        self.child_spans = []  # 暂时无用。完全使用DB中的ParentSpanId。
        self.references = ()  # 暂时无用，类似于节点的边？
