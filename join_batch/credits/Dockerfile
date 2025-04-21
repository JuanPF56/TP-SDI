FROM base_node:latest
WORKDIR /app

# Copy common code
COPY common/ ./common/
COPY join_batch/common/ ./common/

# Copy specific code
COPY join_batch/credits/ .

CMD ["python3", "join_batch.py"]