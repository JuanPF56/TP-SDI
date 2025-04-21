FROM base_node:latest
WORKDIR /app

# Copy common code
COPY common/ ./common/

# Copy specific code
COPY gateway/ .

CMD ["python3", "main.py"]