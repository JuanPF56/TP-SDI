#!/bin/bash

# Contenedores inmunes: no se matan nunca
IMMUNE_CONTAINERS=("proxy" "q1" "q2" "q3" "q4" "q5" "coordinator" "gateway" "rabbitmq")

# Prefijos por tipo de nodo
declare -A NODE_PREFIXES=(
  ["cleanup"]="filter_cleanup"
  ["year"]="filter_year"
  ["production"]="filter_production"
  ["sentiment_analyzer"]="sentiment_analyzer"
  ["join_credits"]="join_credits"
  ["join_ratings"]="join_ratings"
  #["gateway"]="gateway"
)

# # ✅ OPCIONAL: Lista de contenedores específicos para matar (modo dirigido)
# TARGET_CONTAINERS=(
#   "filter_cleanup_6"
#   "filter_year_2"
#   "filter_production_2"
#   "sentiment_analyzer_3"
#   "join_credits_3"
#   "join_ratings_3"
#   "gateway_2"
# )

# Lista de tipos (orden cíclico)
NODE_TYPES=("cleanup" "year" "production" "sentiment_analyzer" "join_credits" "join_ratings" "gateway")
current_type_index=0

kill_nodes() {
  if [ "${#TARGET_CONTAINERS[@]}" -gt 0 ]; then

    # 🔥 Modo dirigidoo: matar un contenedor aleatorio de TARGET_CONTAINERS
    valid_targets=()
    for c in "${TARGET_CONTAINERS[@]}"; do
      if ! [[ "${IMMUNE_CONTAINERS[*]}" =~ "$c" ]]; then
        valid_targets+=("$c")
      fi
    done

    count=${#valid_targets[@]}
    if [ "$count" -gt 0 ]; then
      victim=${valid_targets[$((RANDOM % count))]}
      echo "[!] Matando contenedor específico: $victim"
      docker kill "$victim"
    else
      echo "[i] No hay objetivos válidos en TARGET_CONTAINERS."
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
    echo "[!] Matando contenedor: $victim (tipo: $type)"
    docker kill "$victim"
  else
    echo "[i] No hay suficientes contenedores de tipo $type para matar."
  fi

  # Avanzar al siguiente tipo en el próximo ciclo
  current_type_index=$(( (current_type_index + 1) % ${#NODE_TYPES[@]} ))
}

echo "🚀 Iniciando fault injector. Presioná Ctrl+C para frenar."

# Loop infinito
while true; do
  kill_nodes
  sleep 20
done
