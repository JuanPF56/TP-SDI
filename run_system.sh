#!/bin/bash
set -e

# Valores por defecto
cant_clientes="1"
modo_test="No"
test_config_path=""
default_filename="docker-compose.yaml"

# Si el primer argumento NO empieza con '-', lo tomamos como nombre de archivo
if [[ "$1" != -* && "$#" -ge 1 ]]; then
    filename=$1
    shift
else
    echo "No se especificó nombre de archivo, se usará '$default_filename'"
    filename=$default_filename
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
    esac
    shift
done

echo "🛠️ Instalando dependencias..."
# Verificar si jq está instalado
if ! command -v jq &> /dev/null; then
    echo "⚠️  jq no está instalado. Intentando instalarlo..."

    if command -v apt-get &> /dev/null; then
        sudo apt-get update && sudo apt-get install -y jq
    elif command -v apk &> /dev/null; then
        apk add --no-cache jq
    elif command -v yum &> /dev/null; then
        sudo yum install -y jq
    else
        echo "❌ No se pudo detectar un gestor de paquetes compatible. Por favor instalá jq manualmente."
        exit 1
    fi
else
    echo "✅ jq está instalado."
fi

# Verificar si python y pip están disponibles
if command -v python3 &> /dev/null && command -v pip &> /dev/null; then
    echo "🐍 Instalando dependencias de Python desde requirements.txt..."
    pip install -r requirements.txt
else
    echo "⚠️  Python o pip no están disponibles. Saltando instalación de dependencias Python."
fi

# Ejecutamos download_datasets.py si es modo test
if [ "$modo_test" == "Sí" ]; then
    echo "⚙️ Ejecutando download_datasets.py con -test $test_config_path"
    python3 download_datasets.py -test "$test_config_path"
fi

echo "🛠️ Generando archivo docker compose..."
python3 docker-compose-generator.py "$filename" "${args[@]}"

if [[ ! -f $filename ]]; then
    echo "❌ No se pudo generar el archivo $filename. Asegúrate de que docker-compose-generator.py se ejecutó correctamente."
    exit 1
fi

echo "🛠️ Construyendo imágenes..."
make docker-image

CONFIG_FILE="global_config.ini"

echo "🔍 Leyendo configuración desde $CONFIG_FILE..."
declare -A service_prefixes=(
    [cleanup_filter_nodes]="filter_cleanup_"
    [production_filter_nodes]="filter_production_"
    [year_filter_nodes]="filter_year_"
    [sentiment_analyzer_nodes]="sentiment_analyzer_"
    [join_credits_nodes]="join_credits_"
    [join_ratings_nodes]="join_ratings_"
)

# Servicios core por defecto
services_to_start="rabbitmq gateway q1 q2 q3 q4 q5"
client_services=""

while IFS="=" read -r key value; do
    key=$(echo "$key" | xargs)
    value=$(echo "$value" | xargs)

    if [[ ${service_prefixes[$key]+_} ]]; then
        prefix="${service_prefixes[$key]}"
        for i in $(seq 1 "$value"); do
            service_name="${prefix}${i}"
            if [[ "$key" == "client_nodes" ]]; then
                client_services+=" $service_name"
            else
                services_to_start+=" $service_name"
            fi
        done
    fi
done < <(grep '=' "$CONFIG_FILE" | grep -v '^#')

echo "🚀 Levantando servicios core: $services_to_start"
docker compose -f "$filename" up -d --build $services_to_start

echo "⏳ Esperando a que los servicios estén saludables..."

wait_for_healthy() {
    local retries=30
    local sleep_seconds=2
    local count=0

    while [[ $count -lt $retries ]]; do
        unhealthy=$(docker compose ps --format json 2>/dev/null | jq -r 'select(type == "array") | .[] | select(.Health != null and .Health.Status != "healthy") | .Name')
        if [[ -z "$unhealthy" ]]; then
            echo "✅ Todos los servicios están saludables."
            return 0
        fi

        echo "🔄 Esperando por: $unhealthy"
        sleep "$sleep_seconds"
        ((count++))
    done

    echo "❌ Timeout esperando que los servicios estén saludables."
    docker compose ps
    exit 1
}

wait_for_healthy

if [[ -n "$client_services" ]]; then
    echo "🚀 Levantando clientes: $client_services"
    docker compose -f "$filename" up -d --build $client_services
    wait_for_healthy
fi

echo "✅ Todo el sistema está en ejecución."

echo "📝 Mostrando logs:"
make docker-compose-logs