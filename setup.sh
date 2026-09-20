#!/bin/bash
#
# Prerrequisito: este script asume que ya estas DENTRO del devcontainer
# (terraform/uv/dbt viven ahi, no en el host). Si acabas de clonar el repo
# en VS Code: Cmd/Ctrl+Shift+P -> "Dev Containers: Rebuild and Reopen in
# Container" antes de correr esto.
set -euo pipefail

cd "$(dirname "$0")"

ENV_FILE=".env"

if [ ! -f "$ENV_FILE" ]; then
  echo "Crear .env con las variables necesarias"
  exit 1
fi

set -a
source "$ENV_FILE"
set +a

export TF_VAR_databricks_host="$DATABRICKS_HOST"
export TF_VAR_databricks_token="$DATABRICKS_TOKEN"

rm -rf datos/chunked
mkdir -p datos/raw datos/chunked
cd datos/raw
for day in $(seq 1 7)
do
  padded=$(printf '%02d' "$day")
  if ls | grep -q "AIS_2023_06_${padded}"; then
    echo "AIS_2023_06_${padded} ya existe, salto descarga"
  else
    curl -fsS -C - --http1.1 --retry 5 --retry-delay 3 --retry-all-errors -O "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_${padded}.zip"
  fi
done

if ls | grep -q ".zip"; then
  unzip -o '*.zip' && rm ./*.zip
fi

chunk_and_gzip_day() {
  local day="$1"
  local padded file prefix header part

  padded=$(printf '%02d' "$day")
  file="AIS_2023_06_${padded}.csv"
  prefix="../chunked/AIS_2023_06_${padded}_part_"
  header="header_${padded}.csv"

  head -n 1 "$file" > "$header"
  tail -n +2 "$file" | split -C 100m -d -a 3 --additional-suffix=.csv - "$prefix"

  for part in ${prefix}*.csv; do
    cat "$header" "$part" > "${part}.tmp" && mv "${part}.tmp" "$part"
    gzip -f "$part"
  done
  rm "$header"
}

pids=()
for day in $(seq 1 7)
do
  chunk_and_gzip_day "$day" &
  pids+=($!)
done

fail=0
for pid in "${pids[@]}"; do
  wait "$pid" || fail=1
done

if [ "$fail" -eq 1 ]; then
  echo "Fallo el chunking/compresion de algun dia" >&2
  exit 1
fi

cd ../..

cd infra
terraform init
terraform apply -auto-approve
export DATABRICKS_VOLUME=$(terraform output -json volume_path | jq -r '.landing')
cd ..

time databricks fs cp -r datos/chunked "dbfs:${DATABRICKS_VOLUME}" --concurrency 12