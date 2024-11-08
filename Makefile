docker:
	docker build . registry.cn-beijing.aliyuncs.com/obser/coroot-tracing-algo:latest

serve:
	./venv/bin/prefect server start
