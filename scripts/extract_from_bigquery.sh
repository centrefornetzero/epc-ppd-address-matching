#!/bin/bash

set -eu

bq extract --location EU --destination_format PARQUET \
    "$GCP_PROJECT_ID:prod_epc_ppd_address_matching.dim_epc_addresses" \
    "gs://$SCRATCH_BUCKET_NAME/$USER/dim_epc_addresses-*.parquet"

bq extract --location EU --destination_format PARQUET \
    "$GCP_PROJECT_ID:prod_epc_ppd_address_matching.dim_ppd_addresses" \
    "gs://$SCRATCH_BUCKET_NAME/$USER/dim_ppd_addresses-*.parquet"

mkdir -p data/addresses

gsutil cp "gs://$SCRATCH_BUCKET_NAME/$USER/dim_epc_addresses-*.parquet" data/addresses/
gsutil cp "gs://$SCRATCH_BUCKET_NAME/$USER/dim_ppd_addresses-*.parquet" data/addresses/