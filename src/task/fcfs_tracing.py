import copy
from typing import List

import pandas
from prefect import get_run_logger, task, states

import src.task.traceweaver as tw
from src.task.dto.span import Span
from src.task.init_variables import *


@task(log_prints=True)
def update_children(time_batch_spans, service_names):
    """
    :param time_batch_spans: 是 sid_span_map。
    :param service_names: container id 列表。
    :return:
    """
    if len(time_batch_spans) == 0:
        return states.Failed(message="Empty time batch")

    spans = [s for s in time_batch_spans.values()]

    fcfs = FCFS(spans, service_names)
    in_spans_by_process, out_spans_by_process = aggregate_spans(spans, service_names)

    def update_parent_mock(child_span_id, parent_span_id):
        print(f"span_id mapping: {child_span_id} - {parent_span_id}")

    def update_parent(child_span_id, parent_span_id):
        # 先更新缓存
        if child_span_id in time_batch_spans:
            time_batch_spans[child_span_id].parent_span_id = parent_span_id
        # 后更新DB
        update_sql = f"ALTER TABLE {t_trace} " \
                     f"UPDATE ParentSpanId = \'{parent_span_id}\' " \
                     f"WHERE SpanId = \'{child_span_id}\';"
        try:
            pandas.read_sql_query(update_sql, ch_engine)
        except:
            print(f"Updating mapping failed: ({child_span_id}, {parent_span_id})")

    # 遍历系统中的全体 process
    for process in service_names:
        result = compute_single_process(process, in_spans_by_process, out_spans_by_process, service_names,
                                        spans, time_batch_spans, fcfs)
        if result is None:
            print(f"Failed to compute process {process}")
            continue
        print(f"Started to compute process {process}")

        # 展开 assignment 结构
        for ep, mappings in result.pred_assignments.items():
            for child_sid, parent_sid in mappings.items():
                # update_parent(child_sid[1], parent_sid[1])
                update_parent_mock(child_sid[1], parent_sid[1])

    return states.Completed(message="`update_children` finished")


# 将全量 span 数据按照 service 进行聚合：in_spans 按 callee 聚合，out_spans 按 caller 聚合。
def aggregate_spans(spans, service_names):
    logger = get_run_logger()

    in_spans_by_process = {}
    out_spans_by_process = {}
    for span in spans:
        if span.caller == '' or span.callee == '':
            logger.warning(f"span with unknown service: {span.span_id}")
            continue

        # fixme 现在 caller 是 ip 表示的。
        # fixme 需要系统中所有的进程，哪怕是redis这样没有下游服务的进程。
        if span.caller not in service_names or span.callee not in service_names:
            continue

        if span.callee not in in_spans_by_process:
            in_spans_by_process[span.callee] = []
        in_spans_by_process[span.callee].append(span)

        if span.caller not in out_spans_by_process:
            out_spans_by_process[span.caller] = []
        out_spans_by_process[span.caller].append(span)

    return in_spans_by_process, out_spans_by_process


# 锁定某个 PID 进行计算，“处理一个进程”
def compute_single_process(process, in_spans_by_process, out_spans_by_process, all_processes, all_spans, sid_span_map,
                           predictor):
    # todo 那么边界服务如何进行计算？
    if process not in in_spans_by_process or process not in out_spans_by_process:
        return None

    in_spans = copy.deepcopy(in_spans_by_process[process])
    out_spans = copy.deepcopy(out_spans_by_process[process])

    # 计算分区（partition）的模板
    def PartitionSpansByEndPoint(spans: List[Span], endpoint_lambda):
        partitions = {}
        for span in spans:
            ep = endpoint_lambda(span)
            if ep not in partitions:
                partitions[ep] = []
            partitions[ep].append(span)
        for ep, part in partitions.items():
            part.sort(key=lambda x: (x.start_time, x.end_time))
        return partitions

    # 针对 in_spans，拿的是上游服务的ep。
    in_span_partitions = PartitionSpansByEndPoint(
        in_spans, lambda x: x.GetParentProcess(all_processes, all_spans)
    )
    # 针对 out_spans，拿的是下游服务的ep。
    out_span_partitions = PartitionSpansByEndPoint(
        out_spans, lambda x: x.GetChildProcess(all_processes, all_spans)
    )
    # 当前服务的上游服务不止一个（顶点的入度大于一）
    if len(in_span_partitions.keys()) > 1:
        print("Error DAG Struct")

    true_assignments = tw.GetGroundTruth(in_span_partitions, out_span_partitions)

    call_graph = tw.FindOrder(all_spans, all_processes, in_span_partitions, out_span_partitions, sid_span_map)

    instrumented_hops = []
    true_assignments = None

    result = predictor.FindAssignments(
        process, in_span_partitions, out_span_partitions, True, instrumented_hops, true_assignments, call_graph
    )
    result.acc = tw.AccuracyForService(result.pred_assignments, true_assignments, in_span_partitions)
    return result


class FCFS(object):
    def __init__(self, all_spans, all_processes):
        self.all_spans = all_spans
        self.all_processes = all_processes
        self.parallel = True
        self.instrumented_hops = []
        self.true_assignments = None

    def FindAssignments(
            self, process, in_span_partitions, out_span_partitions, parallel, instrumented_hops, true_assignments,
            call_graph
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
