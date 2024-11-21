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
            service_name,
            span_kind,
    ):
        self.span_id = span_id
        self.trace_id = trace_id
        self.start_time = start_timestamp  # milliseconds 只支持微秒（6位）
        self.duration = duration // 1000  # nanoseconds to milliseconds
        self.end_time = start_timestamp + timedelta(milliseconds=self.duration)
        self.caller = caller
        self.callee = callee
        self.service_name = service_name  # 全局唯一的 service_name，语义类似于 process_id。
        self.span_kind = span_kind  # coroot's span always comes from client-side
        self.parent_span_id = ''
        self.child_spans = []  # 暂时无用。完全使用DB中的ParentSpanId。
        self.references = ()  # 暂时无用，类似于节点的边？
