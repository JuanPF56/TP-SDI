FROM python:3.13-slim
WORKDIR /app

# Install system dependencies
COPY coordinator/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy common code
COPY common/ ./common/

# Copy specific code
COPY coordinator/ .

CMD ["python3", "coordinator.py"]