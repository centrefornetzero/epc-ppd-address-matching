import argparse

import pandas as pd

from matching.candidates import (
    add_candidate_pair_features,
    add_similarity_score,
    get_candidate_pairs,
)
from matching.matches import get_matches, tabulate_matches
from matching.preprocess import (
    add_epc_features,
    add_ppd_features,
    casefold_epc_addresses,
    casefold_ppd_addresses,
)

MIN_SCORE = 0.5


def get_args(args=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("epc_path")
    parser.add_argument("ppd_path")
    parser.add_argument("output_path")
    return parser.parse_args(args)


if __name__ == "__main__":
    args = get_args()

    print("Loading EPC addresses...")
    epc_addresses = (
        pd.read_parquet(args.epc_path)
        .pipe(casefold_epc_addresses)
        .pipe(add_epc_features)
    )
    print(f"{len(epc_addresses)} EPC addresses")

    print("Loading PPD addresses...")
    ppd_addresses = (
        pd.read_parquet(args.ppd_path)
        .pipe(casefold_ppd_addresses)
        .pipe(add_ppd_features)
    )
    print(f"{len(ppd_addresses)} PPD addresses")

    print("Generating candidate pairs...")
    candidates = (
        get_candidate_pairs(epc_addresses, ppd_addresses)
        .pipe(add_candidate_pair_features)
        .pipe(add_similarity_score)
    )
    print(f"{len(candidates)} candidate pairs")

    matches = list(get_matches(candidates, MIN_SCORE))
    num_records_matched = len(
        [node for connected_component in matches for node in connected_component]
    )
    print(
        f"{len(matches)} connected components containing {num_records_matched} records"
    )

    tabulate_matches(matches).reset_index().to_parquet(args.output_path)
