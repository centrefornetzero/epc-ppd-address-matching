import pickle

import pandas as pd

from matching.candidates import (
    add_candidate_pair_features,
    add_similarity_score,
    get_candidate_pairs,
)
from matching.matches import get_matches
from matching.preprocess import (
    add_epc_features,
    add_ppd_features,
    casefold_epc_addresses,
    casefold_ppd_addresses,
)

MIN_SCORE = 0.5
POSTCODE_PREFIX = "E17 "

if __name__ == "__main__":
    print("Loading EPC addresses...")
    epc_addresses = (
        pd.read_parquet("data/epc_addresses")
        .query("postcode.str.startswith(@POSTCODE_PREFIX)")
        .pipe(casefold_epc_addresses)
        .pipe(add_epc_features)
    )
    print(f"{len(epc_addresses)} EPC addresses")

    print("Loading PPD addresses...")
    ppd_addresses = (
        pd.read_parquet("data/ppd_addresses")
        .query("postcode.str.startswith(@POSTCODE_PREFIX)")
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

    with open("matches.pickle", "wb") as f:
        pickle.dump(matches, f, pickle.HIGHEST_PROTOCOL)
