FROM python:3.13-slim
WORKDIR /app

# Copy common code
COPY common/ ./common/
COPY join_batch/common/ ./common/

# Copy specific code
COPY join_batch/ratings/ .

# Install dependencies
RUN pip install pika

CMD ["python3", "join_batch.py"]