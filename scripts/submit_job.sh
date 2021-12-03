#!/bin/bash

set -eu

cat > config.yml <<EOF
workerPoolSpecs:
  machineSpec:
    machineType: e2-standard-16
  replicaCount: 1
  containerSpec:
    imageUri: $IMAGE_URI
    env:
     - name: GCP_PROJECT_ID
       value: $GCP_PROJECT_ID
     - name: SCRATCH_BUCKET_NAME
       value: $SCRATCH_BUCKET_NAME
     - name: USER
       value: epc-ppd-matching
serviceAccount: $SERVICE_ACCOUNT
EOF

gcloud ai custom-jobs create \
    --region europe-west2 \
    --display-name epc-ppd-address-matching \
    --config config.yml

