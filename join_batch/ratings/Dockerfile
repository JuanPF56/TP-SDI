FROM python:3.13-slim
WORKDIR /app

# Copy common code
COPY ../common/ ./common/
COPY ../../common/ ./common/

# Copy specific code
COPY join_batch.py .

CMD ["python3", "join_batch.py"]