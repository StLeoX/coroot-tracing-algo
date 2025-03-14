import pickle

from prefect.logging import disable_run_logger

if __name__ == '__main__':
    sid_span_map = dict()
    with open("/root/Source/obser/coroot-tracing-algo/test/testdata/dataset1.pkl", "rb") as f:
        sid_span_map = pickle.load(f)
        assert 50, len(sid_span_map)

    service_names = ['172.20.0.8', '172.20.0.5', '172.20.0.2', '172.20.0.6']

    from src.task.traceweaver_tracing import update_children

    with disable_run_logger():
        update_children.fn(sid_span_map, service_names)
