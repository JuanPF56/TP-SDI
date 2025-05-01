#!/bin/bash

if [ "$#" -lt 1 ]; then
    echo "Uso: $0 <nombre_archivo> [-cant_clientes <numero>] [-short_test]"
    exit 1
fi

filename=$1
shift

args=("$@")  # Guardamos todos los flags originales

# Valores por defecto
cant_clientes="1"
modo_test="No"

# Parseo manual de los flags
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        -cant_clientes)
            shift
            cant_clientes=$1
            ;;
        -short_test)
            modo_test="Sí"
            ;;
    esac
    shift
done

echo "Generando archivo docker compose..."
echo "Nombre del archivo de salida: $filename"
echo "Cantidad de clientes: $cant_clientes"
echo "¿Modo test activado?: $modo_test"

# Usamos los argumentos originales guardados
python3 docker-compose-generator.py "$filename" "${args[@]}"
echo "Archivo generado con éxito."
