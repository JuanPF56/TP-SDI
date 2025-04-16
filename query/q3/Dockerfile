FROM python:3.13-slim
WORKDIR /app

# Copy common code
COPY common/ ./common/

# Copy specific code
COPY queries/q3/ .

CMD ["python3", "q3.py"]