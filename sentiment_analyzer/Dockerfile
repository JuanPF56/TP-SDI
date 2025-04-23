# Usa una imagen base de Hugging Face que ya tiene transformers y torch
FROM huggingface/transformers-pytorch-cpu

# Usa Python 3.9 explícitamente (si es posible, verificar en base image)
# Ya viene instalado en esta imagen base

# Establece el directorio de trabajo
WORKDIR /app

# Copia el código fuente
COPY sentiment_analyzer/ ./
COPY common/ ./common/

# Instalá las dependencias
RUN pip install --no-cache-dir pika

# ⚠️ Predescarga el modelo para evitar fallos al iniciar
RUN python3 -c "from transformers import pipeline; pipeline('sentiment-analysis')"

# Ejecuta el servicio
CMD ["python3", "sentiment_analyzer.py"]
