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

kill_random_node() {
  for type in "${!NODE_PREFIXES[@]}"; do
    prefix="${NODE_PREFIXES[$type]}"
    
    # Obtener contenedores de este tipo (excluyendo los inmunes)
    containers=($(docker ps --format '{{.Names}}' | grep "^$prefix" | grep -v -E "$(IFS='|'; echo "${IMMUNE_CONTAINERS[*]}")"))

    count=${#containers[@]}
    if [ "$count" -gt 1 ]; then
      # Elegir uno aleatorio para matar
      victim=${containers[$((RANDOM % count))]}
      echo "[!] Matando contenedor: $victim (tipo: $type)"
      docker kill "$victim"
      return
    fi
  done

  echo "[i] No hay nodos redundantes para matar en este ciclo."
}

echo "🚀 Iniciando fault injector. Presioná Ctrl+C para frenar."

# Loop infinito
while true; do
  kill_random_node
  sleep 60
done
