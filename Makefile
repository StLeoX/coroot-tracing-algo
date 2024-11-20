docker:
	docker build . registry.cn-beijing.aliyuncs.com/obser/coroot-tracing-algo:latest

server:
	./venv/bin/prefect server start &

deploy:
	python ./src/main.py
