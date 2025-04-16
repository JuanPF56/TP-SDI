#!/bin/bash

# Chequeo de parametros
if [ "$#" -ne 1 ]; then
    echo "Uso: $0 <nombre_archivo>"
    exit 1
fi

filename=$1

echo "Generando archivo docker compose..."
echo "Nombre del archivo de salida: $filename"

python3 generador-compose.py $filename
echo "Archivo generado con éxito."