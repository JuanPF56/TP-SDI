FROM python:3.13-slim
WORKDIR /app

# Copy common code
COPY common/ ./common/

# Copy specific code
COPY join_table/ .

CMD ["python3", "join_table.py"]