docker:
	docker build . -t registry.cn-beijing.aliyuncs.com/obser/coroot-tracing-algo:v1.25.0

server:
	./venv/bin/prefect server start &

deploy:
	python ./src/main.py
