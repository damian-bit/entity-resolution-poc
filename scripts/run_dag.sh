#!/usr/bin/env bash
# Uso: scripts/run_dag.sh <run_id> ['<conf json>']  — dispara el DAG y espera el resultado.
set -euo pipefail
RUN_ID="$1"; CONF="${2:-{\}}"
AF="docker compose exec -T airflow airflow"
$AF dags unpause entity_resolution >/dev/null
$AF dags trigger entity_resolution --run-id "$RUN_ID" --conf "$CONF" >/dev/null
echo "Run $RUN_ID disparado (conf=$CONF). Esperando..."
until STATE=$($AF dags state entity_resolution "$RUN_ID" 2>/dev/null | tail -1); [[ "$STATE" == *success* || "$STATE" == *failed* ]]; do
  sleep 15
done
echo "Run $RUN_ID: $STATE"
$AF tasks states-for-dag-run entity_resolution "$RUN_ID" -o plain 2>/dev/null | awk 'NR>1 {print $3, $4}' | sort | uniq -c
[[ "$STATE" == *success* ]]
