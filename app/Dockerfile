FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common.py producer.py consumer.py ./
USER nobody
CMD ["python", "-u", "producer.py"]
