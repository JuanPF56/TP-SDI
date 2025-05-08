FROM base_node:latest
WORKDIR /app

# Copy common code
COPY common/ ./common/
COPY query/common/ ./common/

# Copy specific code
COPY query/q2/ .

CMD ["python3", "q2.py"]