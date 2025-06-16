FROM base_node:latest
WORKDIR /app

# Install system dependencies
COPY coordinator/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy common code
COPY common/ ./common/

# Copy specific code
COPY gateway/ .

CMD ["python3", "main.py"]