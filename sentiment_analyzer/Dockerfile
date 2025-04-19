FROM huggingface/transformers-pytorch-cpu

WORKDIR /app

# Copiá todo el código necesario
COPY sentiment_analyzer/ .
COPY common/ ./common/

# Instalá dependencias adicionales
RUN pip install pika

CMD ["python3", "sentiment_analyzer.py"]
