from prefect import task, states

import src.task.traceweaver as tw
import src.task.utils as utils


@task(log_prints=True)
def update_children(time_batch_spans):
    """
    :param time_batch_spans: 是 sid_span_map。
    """
    if len(time_batch_spans) == 0:
        return states.Failed(message="Empty time batch")

    spans = time_batch_spans.values()

    service_names = tw.GetServiceNames(spans)

    fcfs = FCFS(spans, service_names)
    in_spans_by_process, out_spans_by_process = tw.AggregateSpans(spans, service_names)

    # 遍历系统中的全体 process
    for process in service_names:
        result = tw.ComputeSingleProcess(process, in_spans_by_process, out_spans_by_process, service_names,
                                         spans, time_batch_spans, fcfs)
        if result is None:
            print(f"Failed to compute process {process}")
            continue
        print(f"Computed process {process}")

        # 展开 assignment 结构
        for ep, mappings in result.pred_assignments.items():
            for child_sid, parent_sid in mappings.items():
                # update_parent(child_sid[1], parent_sid[1])
                utils.update_parent_mock(time_batch_spans[child_sid[1]], parent_sid[1])

    return states.Completed(message="`update_children` finished")


class FCFS(object):
    def __init__(self, all_spans, all_processes):
        self.all_spans = all_spans
        self.all_processes = all_processes
        self.parallel = True
        self.instrumented_hops = []
        self.true_assignments = None

    def FindAssignments(
            self, process, in_span_partitions, out_span_partitions, parallel, instrumented_hops, true_assignments
    ):
        assert len(in_span_partitions) == 1
        self.instrumented_hops = instrumented_hops
        self.true_assignments = true_assignments
        _, in_spans = list(in_span_partitions.items())[0]
        all_assignments = {ep: {} for ep in out_span_partitions.keys()}
        for ind in range(len(in_spans)):
            for j, (ep, out_spans) in enumerate(out_span_partitions.items()):
                if ind >= len(out_spans):
                    all_assignments[ep][in_spans[ind].GetId()] = ("NA", "NA")
                    continue
                if (j + 1) in instrumented_hops:
                    all_assignments[ep][in_spans[ind].GetId()] = self.true_assignments[ep][in_spans[ind].GetId()]
                else:
                    all_assignments[ep][in_spans[ind].GetId()] = out_spans[ind].GetId()
        return tw.FindAssignmentsResult(all_assignments, None, None, None)
