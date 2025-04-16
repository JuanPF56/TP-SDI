FROM python:3.13-slim
WORKDIR /app

# Copy common code
COPY common/ ./common/

# Copy specific code
COPY client/ .

CMD ["python3", "client.py"]