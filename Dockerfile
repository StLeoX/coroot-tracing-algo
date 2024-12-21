FROM python:3.10.6-slim-bullseye AS basic

WORKDIR /coroot

COPY ./requirements.txt .

RUN pip install -i "https://mirrors.aliyun.com/pypi/simple/" -r requirements.txt

FROM basic

WORKDIR /coroot

COPY . .

EXPOSE 4200

ENTRYPOINT ["python", "-m", "src.main"]
