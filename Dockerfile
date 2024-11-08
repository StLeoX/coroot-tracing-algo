FROM prefecthq/prefect:3.1.0-python3.10

WORKDIR /coroot

COPY . /coroot

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8889

# todo start prefect
CMD ["python", "main.py"]
