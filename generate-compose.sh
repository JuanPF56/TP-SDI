#!/bin/bash

if [ "$#" -lt 1 ]; then
    echo "Uso: $0 <nombre_archivo> [-cant_clientes <numero>] [-short_test]"
    exit 1
fi

filename=$1
shift

echo "Generando archivo docker compose..."
echo "Nombre del archivo de salida: $filename"

python3 docker-compose-generator.py "$filename" "$@"
echo "Archivo generado con éxito."
