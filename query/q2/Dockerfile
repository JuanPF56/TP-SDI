FROM python:3.13-slim
WORKDIR /app

# Copy common code
COPY common/ ./common/

# Copy specific code
COPY query/q2/ .

CMD ["python3", "q2.py"]