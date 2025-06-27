#!/bin/bash

# Función para obtener timestamp (+3 horas UTC-3 -> UTC)
get_timestamp() {
    date -d '+3 hours' '+[%H:%M:%S]'
}

# Contenedores inmunes: no se matan nunca
IMMUNE_CONTAINERS=("proxy" "q1" "q2" "q3" "q4" "q5" "rabbitmq" "client")

# Prefijos por tipo de nodo
declare -A NODE_PREFIXES=(
    ["cleanup"]="filter_cleanup"
    ["year"]="filter_year"
    ["production"]="filter_production"
    ["sentiment_analyzer"]="sentiment_analyzer"
    ["join_credits"]="join_credits"
    ["join_ratings"]="join_ratings"
    ["gateway"]="gateway"
    ["coordinator"]="coordinator"
)

# ✅ OPCIONAL: Lista de contenedores específicos para matar (modo dirigido)
# TARGET_CONTAINERS=(
#     "filter_cleanup_6"
#     "filter_year_2"
#     "filter_production_2"
#     "sentiment_analyzer_3"
#     "join_credits_3"
#     "join_ratings_3"
#     "gateway_2"
# )

# Lista de tipos (orden cíclico)
NODE_TYPES=("cleanup" "year" "production" "sentiment_analyzer" "join_credits" "join_ratings" "gateway" "coordinator")
current_type_index=0

# Archivo de log opcional (descomenta si quieres guardar en archivo)
LOG_FILE="fault_injector.log"

log_message() {
    local message="$1"
    local timestamp=$(get_timestamp)
    local full_message="$timestamp $message"
    
    # Mostrar en consola
    echo "$full_message"
    
    # Opcional: también guardar en archivo
    # echo "$full_message" >> "$LOG_FILE"
}

kill_nodes() {
    if [ "${#TARGET_CONTAINERS[@]}" -gt 0 ]; then
        # 🔥 Modo dirigido: matar un contenedor aleatorio de TARGET_CONTAINERS
        valid_targets=()
        for c in "${TARGET_CONTAINERS[@]}"; do
            if ! [[ "${IMMUNE_CONTAINERS[*]}" =~ "$c" ]]; then
                valid_targets+=("$c")
            fi
        done
        
        count=${#valid_targets[@]}
        if [ "$count" -gt 0 ]; then
            victim=${valid_targets[$((RANDOM % count))]}
            log_message "[KILL] Matando contenedor específico: $victim"
            docker kill "$victim"
            if [ $? -eq 0 ]; then
                log_message "[SUCCESS] Contenedor $victim eliminado exitosamente"
            else
                log_message "[ERROR] Falló al eliminar contenedor $victim"
            fi
        else
            log_message "[INFO] No hay objetivos válidos en TARGET_CONTAINERS."
        fi
        return
    fi

    # 🔁 Modo rotativo: matar un contenedor de un tipo diferente cada vez
    type="${NODE_TYPES[$current_type_index]}"
    prefix="${NODE_PREFIXES[$type]}"
    
    containers=($(docker ps --format '{{.Names}}' | grep "^$prefix" | grep -v -E "$(IFS='|'; echo "${IMMUNE_CONTAINERS[*]}")"))
    count=${#containers[@]}
    
    if [ "$count" -gt 1 ]; then
        victim=${containers[$((RANDOM % count))]}
        log_message "[KILL] Matando contenedor: $victim (tipo: $type, contenedores disponibles: $count)"
        docker kill "$victim"
        if [ $? -eq 0 ]; then
            log_message "[SUCCESS] Contenedor $victim eliminado exitosamente"
        else
            log_message "[ERROR] Falló al eliminar contenedor $victim"
        fi
    else
        log_message "[INFO] No hay suficientes contenedores de tipo '$type' para matar (disponibles: $count)."
    fi

    # Avanzar al siguiente tipo en el próximo ciclo
    current_type_index=$(( (current_type_index + 1) % ${#NODE_TYPES[@]} ))
}

# Función para manejar Ctrl+C gracefully
cleanup() {
    log_message "[SHUTDOWN] Fault injector detenido por usuario"
    exit 0
}

# Capturar Ctrl+C
trap cleanup SIGINT SIGTERM

log_message "[START] 🚀 Iniciando fault injector. Presioná Ctrl+C para frenar."

# En la primera ronda, matar 4 contenedores
log_message "[PHASE] Fase inicial: eliminando 4 contenedores"
for i in {1..4}; do
    log_message "[ROUND] Ronda inicial $i/4"
    kill_nodes
    sleep 2  # Pausa breve entre eliminaciones iniciales
done

log_message "[PHASE] Iniciando ciclo infinito (1 contenedor cada 15 segundos)"

# Luego, continuar con el ciclo infinito matando 1 contenedor por vez
while true; do
    kill_nodes
    log_message "[WAIT] Esperando 15 segundos hasta próxima eliminación..."
    sleep 15
done