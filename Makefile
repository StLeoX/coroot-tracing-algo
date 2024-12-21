.PHONY: docker
docker:
	docker build . -t registry.cn-beijing.aliyuncs.com/obser/coroot-tracing-algo:latest

.PHONY:serve-dev
serve-dev:
	./venv/bin/prefect server stop
	./venv/bin/prefect server start -b
	./venv/bin/python -m src.main
