class Trace:
    def __init__(
        self,
        root_span_id,
        spans,
        cg_signature,
        processes
    ):
        self.root_span_id = root_span_id
        self.spans = spans
        self.cg_signature = cg_signature
        self.processes = processes
