FROM base_node:latest
WORKDIR /app

# Copy common code
COPY common/ ./common/
COPY join/common/ ./common/

# Copy specific code
COPY join/credits/ .

CMD ["python3", "join.py"]