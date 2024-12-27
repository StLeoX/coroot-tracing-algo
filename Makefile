docker:
	docker build . -t registry.cn-beijing.aliyuncs.com/obser/coroot-tracing-algo:v1.24.1-fix

server:
	./venv/bin/prefect server start &

deploy:
	python ./src/main.py
