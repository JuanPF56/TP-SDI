FROM python:3.13-slim
WORKDIR /app

# Copy common code
COPY common/ ./common/

# Copy specific code
COPY sentiment_analyzer/ .

CMD ["python3", "sentiment_analyzer.py"]