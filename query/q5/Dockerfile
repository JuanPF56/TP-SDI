FROM python:3.13-slim
WORKDIR /app

# Copy common code
COPY common/ ./common/

# Copy specific code
COPY query/q5/ .
RUN pip install pika

CMD ["python3", "q5.py"]