FROM base_node:latest
WORKDIR /app

# Install TextBlob and its corpora
RUN pip install --no-cache-dir pika textblob && \
    python3 -m textblob.download_corpora

# Copy common code
COPY sentiment_analyzer/ ./
COPY common/ ./common/

CMD ["python3", "sentiment_analyzer.py"]
