FROM python:3.13-slim
WORKDIR /app

# Copy common code
COPY common/ ./common/

# Copy specific code
COPY query/q4/ .

CMD ["python3", "q4.py"]