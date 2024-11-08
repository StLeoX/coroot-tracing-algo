class Span:
    def __init__(
            self,
            trace_id,
            span_id,
            start_timestamp,
            duration,
            caller,
            callee,
            references,
            process_id,
            span_kind,
    ):
        self.span_id = span_id
        self.trace_id = trace_id
        self.start_mus = start_timestamp
        self.duration_mus = duration
        self.caller = caller
        self.callee = callee
        self.references = references
        self.process_id = process_id
        self.span_kind = span_kind
        self.child_spans = []
