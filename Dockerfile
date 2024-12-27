FROM python:3.10.6-slim-bullseye AS basic

WORKDIR /coroot

RUN python -m venv venv

COPY ./requirements.txt .

RUN ./venv/bin/pip install -i "https://mirrors.aliyun.com/pypi/simple/" --no-cache-dir -r requirements.txt

FROM basic

WORKDIR /coroot

COPY . .

RUN mkdir -p ~/.prefect && cp ./profiles.toml ~/.prefect

# 一些必要的环境变量
ENV COROOT_CLICKHOUSE_ADDRESS ''
ENV COROOT_CLICKHOUSE_PASSWORD ''
ENV COROOT_TRACING_INTERVAL ''
ENV COROOT_TRACING_INTERVAL ''

EXPOSE 4200

CMD ["./entrypoint.sh"]
