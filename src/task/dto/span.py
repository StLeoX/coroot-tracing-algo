from datetime import timedelta, datetime


class Span:
    '''
    有关时间单位的统一：数据库存的是9位时间，内存存的是6位时间（Python仅支持6位）。
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
        self.span_id: str = span_id
        self.trace_id: str = trace_id
        self.parent_span_id: str = ''

        if type(timestamp_us) == str:
            timestamp_us = datetime.strptime(timestamp_us, '%Y-%m-%d %H:%M:%S.%f')
        self.start_time = timestamp_us  # milliseconds
        self.duration = duration_us  # milliseconds
        self.end_time = timestamp_us + timedelta(milliseconds=self.duration)
        self.caller = caller  # network IP
        self.callee = callee  # network IP
        self.container_id = container_id  # 全局唯一的 container_id，类似于 process_id。
        # self.span_kind = span_kind  # coroot's span always comes from client-side

        self.child_spans = []  # 暂时无用。完全使用DB中的ParentSpanId。
        self.references = ()  # 暂时无用，类似于节点的边？
