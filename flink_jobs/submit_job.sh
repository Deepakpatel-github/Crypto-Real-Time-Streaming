#!/bin/bash
set -euo pipefail

FLINK_HOST="${FLINK_JOBMANAGER_HOST:-flink-jobmanager}"
FLINK_PORT="${FLINK_JOBMANAGER_PORT:-8081}"
KAFKA_HOST="${KAFKA_HOST:-kafka}"
KAFKA_PORT="${KAFKA_PORT:-29092}"
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-clickhouse}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-180}"

wait_for_http() {
  local name="$1"
  local url="$2"
  local elapsed=0

  echo "waiting_for=${name} url=${url}"
  until curl -sf "${url}" >/dev/null; do
    sleep 3
    elapsed=$((elapsed + 3))
    if [ "${elapsed}" -ge "${MAX_WAIT_SECONDS}" ]; then
      echo "timeout_waiting_for=${name}"
      exit 1
    fi
  done
  echo "ready=${name}"
}

wait_for_tcp() {
  local name="$1"
  local host="$2"
  local port="$3"
  local elapsed=0

  echo "waiting_for=${name} host=${host} port=${port}"
  until nc -z "${host}" "${port}"; do
    sleep 3
    elapsed=$((elapsed + 3))
    if [ "${elapsed}" -ge "${MAX_WAIT_SECONDS}" ]; then
      echo "timeout_waiting_for=${name}"
      exit 1
    fi
  done
  echo "ready=${name}"
}

wait_for_http "flink" "http://${FLINK_HOST}:${FLINK_PORT}/overview"
wait_for_tcp "kafka" "${KAFKA_HOST}" "${KAFKA_PORT}"
wait_for_tcp "clickhouse" "${CLICKHOUSE_HOST}" "9000"

sleep 15

echo "submitting_flink_job=/opt/flink/jobs/market_processor.py"
/opt/flink/bin/flink run \
  -d \
  -m "${FLINK_HOST}:${FLINK_PORT}" \
  -py /opt/flink/jobs/market_processor.py \
  -pyfs /opt/flink/jobs \
  -pyexec /usr/bin/python3 \
  -pyclientexec /usr/bin/python3

echo "flink_job_submitted"
