#!/bin/bash

set -eu

printf '[Credentials]\ngs_service_key_file=%s' "${GOOGLE_APPLICATION_CREDENTIALS}" > ~/.boto

echo "==> Erasing bucket working directory..."

gsutil ls "gs://$SCRATCH_BUCKET_NAME/*" 1> /dev/null  # to check authorisation

if gsutil ls "gs://$SCRATCH_BUCKET_NAME/$USER/*"; then
    gsutil rm "gs://$SCRATCH_BUCKET_NAME/$USER/*"
fi

echo "==> Starting BQ query and extract jobs..."

python extract_data.py "gs://$SCRATCH_BUCKET_NAME/$USER"

echo "==> Downloading extracts from GCS..."

mkdir -p data/epc_addresses
mkdir -p data/ppd_addresses

gsutil cp "gs://$SCRATCH_BUCKET_NAME/$USER/epc_addresses-*.parquet" data/epc_addresses/
gsutil cp "gs://$SCRATCH_BUCKET_NAME/$USER/ppd_addresses-*.parquet" data/ppd_addresses/

echo "==> Partitioning data..."

python -m matching.partition data/epc_addresses data/partitioned_epc_addresses
python -m matching.partition data/ppd_addresses data/partitioned_ppd_addresses

echo "==> Matching records..."

mkdir -p data/matches

parallel --link --tagstring "Job {#}" \
    python -m matching --cluster-prefix "{#}-" {} "data/matches/matches-{#}.parquet" \
    ::: "$(ls -d data/partitioned_epc_addresses/**)" \
    ::: "$(ls -d data/partitioned_ppd_addresses/**)"

echo "==> Uploading to GCS..."

gsutil cp data/matches/matches-*.parquet "gs://$SCRATCH_BUCKET_NAME/$USER/"

echo "==> Loading into BigQuery..."

bq load --project_id "$GCP_PROJECT_ID" --source_format=PARQUET --replace --location EU \
    src_epc_ppd_address_matching.matches \
    "gs://$SCRATCH_BUCKET_NAME/$USER/matches-*.parquet"
