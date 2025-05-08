FROM base_node:latest

WORKDIR /app

# Dependencys
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    torch \
    transformers \
    pika

RUN python3 -c "from transformers import pipeline; pipeline('sentiment-analysis')"

COPY sentiment_analyzer/ ./ 
COPY common/ ./common/
COPY sentiment_analyzer/benchmark.py /app/benchmark.py

CMD ["python3", "sentiment_analyzer.py"]
