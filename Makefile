.PHONY: docker
docker:
	docker build . -t registry.cn-beijing.aliyuncs.com/obser/coroot-tracing-algo:latest

.PHONY:serve-dev
serve-dev:
	./venv/bin/prefect server start -b
	./venv/bin/python -m src.main

.PHONY:serve-prod
serve-prod:
	prefect server start -b
	prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
	python -m src.main
