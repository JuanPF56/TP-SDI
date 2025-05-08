FROM base_node:latest
WORKDIR /app

# Copy common code
COPY common/ ./common/
COPY query/common/ ./common/

# Copy specific code
COPY query/q4/ .

CMD ["python3", "q4.py"]