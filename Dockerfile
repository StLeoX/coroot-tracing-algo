FROM prefecthq/prefect:3.1.0-python3.10

WORKDIR /coroot

COPY . /coroot

RUN pip install -i "https://mirrors.aliyun.com/pypi/simple/" -r requirements.txt

EXPOSE 4200

ENTRYPOINT ["make", "serve-prod"]
