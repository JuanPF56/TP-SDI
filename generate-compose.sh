#!/bin/bash

if [ "$#" -lt 1 ]; then
    echo "Uso: $0 <nombre_archivo> [-cant_clientes <numero>] [-test <test_config_path>]"
    exit 1
fi

filename=$1
shift

args=("$@")  # Guardamos todos los flags originales

# Valores por defecto
cant_clientes="1"
modo_test="No"
test_config_path=""

# Parseo manual de los flags
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        -cant_clientes)
            shift
            cant_clientes=$1
            ;;
        -test)
            modo_test="Sí"
            shift
            test_config_path=$1
            ;;
    esac
    shift
done

echo "Generando archivo docker compose..."
echo "Nombre del archivo de salida: $filename"
echo "Cantidad de clientes: $cant_clientes"
echo "¿Modo test activado?: $modo_test"

# Ejecutamos download_datasets.py si es modo test
if [ "$modo_test" == "Sí" ]; then
    echo "Ejecutando download_datasets.py con -test $test_config_path"
    python3 download_datasets.py -test "$test_config_path"
fi

# Generamos el archivo de docker compose
python3 docker-compose-generator.py "$filename" "${args[@]}"
echo "Archivo generado con éxito."
