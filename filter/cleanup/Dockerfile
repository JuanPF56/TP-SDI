FROM python:3.13-slim
WORKDIR /app

# Copy common code
COPY common/ ./common/
COPY filter/common/ ./common/

# Copy specific code
COPY filter/cleanup/ .

CMD ["python3", "filter.py"]