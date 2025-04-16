FROM python:3.13-slim
WORKDIR /app

# Copy common code
COPY ../common/ ./common/

# Copy specific code
COPY q1.py .

CMD ["python3", "q1.py"]