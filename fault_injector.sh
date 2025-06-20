#!/bin/bash

# Contenedores inmunes: no se matan nunca
IMMUNE_CONTAINERS=("proxy" "q1" "q2" "q3" "q4" "q5" "coordinator")

# Prefijos por tipo de nodo
declare -A NODE_PREFIXES=(
  ["cleanup"]="filter_cleanup"
  ["year"]="filter_year"
  ["production"]="filter_production"
  ["sentiment_analyzer"]="sentiment_analyzer"
  ["join_credits"]="join_credits"
  ["join_ratings"]="join_ratings"
  ["gateway"]="gateway"
)

# ✅ OPCIONAL: Lista de contenedores específicos para matar (modo dirigido)
# TARGET_CONTAINERS=(
#   "filter_cleanup_6"
#   "filter_year_2"
#   "filter_production_2"
#   "sentiment_analyzer_3"
#   "join_credits_3"
#   "join_ratings_3"
# )
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
      echo "[!] Matando contenedor específico: $victim"
      docker kill "$victim"
    else
      echo "[i] No hay objetivos válidos en TARGET_CONTAINERS."
    fi
    return
  fi

  # 🔁 Modo automático: matar un nodo por tipo si hay más de uno
  killed_any=false
  for type in "${!NODE_PREFIXES[@]}"; do
    prefix="${NODE_PREFIXES[$type]}"
    
    containers=($(docker ps --format '{{.Names}}' | grep "^$prefix" | grep -v -E "$(IFS='|'; echo "${IMMUNE_CONTAINERS[*]}")"))

    count=${#containers[@]}
    if [ "$count" -gt 1 ]; then
      victim=${containers[$((RANDOM % count))]}
      echo "[!] Matando contenedor: $victim (tipo: $type)"
      docker kill "$victim"
      killed_any=true
    fi
  done

  if ! $killed_any; then
    echo "[i] No hay nodos redundantes para matar en este ciclo."
  fi
}

echo "🚀 Iniciando fault injector. Presioná Ctrl+C para frenar."

# Loop infinito
while true; do
  kill_nodes
  sleep 20
done
