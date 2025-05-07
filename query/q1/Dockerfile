FROM base_node:latest
WORKDIR /app

# Copy common code
COPY common/ ./common/
COPY query/common/ ./common/

# Copy specific code
COPY query/q1/ .

CMD ["python3", "q1.py"]