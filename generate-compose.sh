#!/bin/bash

# Valores por defecto
cant_clientes="1"
modo_test="No"
test_config_path=""
skip_download="No"
default_filename="docker-compose"

# Si el primer argumento NO empieza con '-', lo tomamos como nombre base
if [[ "$1" != -* && "$#" -ge 1 ]]; then
    base_filename=$1
    shift
else
    echo "No se especificó nombre base, se usará '$default_filename'"
    base_filename=$default_filename
fi

args=("$@")  # Guardamos todos los flags originales

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
        -skip_download)
            skip_download="Sí"
            ;;
    esac
    shift
done

echo "Generando archivos docker compose..."
echo "Nombre base: $base_filename"
echo "Cantidad de clientes: $cant_clientes"
echo "¿Modo test activado?: $modo_test"
echo "¿Saltear descarga de datasets?: $skip_download"

# Descargar datasets completos si no está en modo test y no se quiere saltear la descarga
if [ "$modo_test" == "No" ] && [ "$skip_download" == "No" ]; then
    echo "Descargando datasets completos..."
    python download_datasets.py
fi

# Reducir datasets si está el flag -test
if [ "$modo_test" == "Sí" ]; then
    echo "Reduciendo datasets con -test $test_config_path"
    python download_datasets.py -test "$test_config_path"
fi

# Generamos archivo del sistema
echo "Generando $base_filename.system.yml"
python docker-compose-generator.py system "$base_filename.system.yml"

# Generamos archivo de clientes
echo "Generando $base_filename.clients.yml"
client_args=("clients" "$base_filename.clients.yml" "-cant_clientes" "$cant_clientes")
if [ "$modo_test" == "Sí" ]; then
    client_args+=("-test" "$test_config_path")
fi

python docker-compose-generator.py "${client_args[@]}"

echo "Archivos generados con éxito:"
echo "  -> $base_filename.system.yml"
echo "  -> $base_filename.clients.yml"