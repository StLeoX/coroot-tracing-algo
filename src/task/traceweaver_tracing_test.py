import pickle

from prefect.logging import disable_run_logger

if __name__ == '__main__':
    sid_span_map = dict()
    with open("/root/Source/obser/coroot-tracing-algo/test/testdata/dataset2.pkl", "rb") as f:
        sid_span_map = pickle.load(f)

    from src.task.traceweaver_tracing import update_children

    with disable_run_logger():
        update_children.fn(sid_span_map)
