#!/bin/bash

set -eu

python -m matching.partition data/epc_addresses data/partitioned_epc_addresses

python -m matching.partition data/ppd_addresses data/partitioned_ppd_addresses

parallel --link --tagstring "Job {#}" \
    python -m matching --cluster-prefix "{#}-" {} "data/matches-{#}.parquet" \
    ::: "$(ls -d data/partitioned_epc_addresses/**)" \
    ::: "$(ls -d data/partitioned_ppd_addresses/**)"
