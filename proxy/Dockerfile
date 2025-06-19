FROM python:3.13-slim
WORKDIR /app

# Install system dependencies
COPY client/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy common code
COPY common/ ./common/

# Copy specific code
COPY proxy/ .

CMD ["python3", "main.py"]