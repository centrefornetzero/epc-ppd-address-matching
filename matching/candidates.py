from typing import Optional

import pandas as pd

__all__ = ["get_candidate_pairs", "add_candidate_pair_features"]


def get_candidate_pairs(
    epc_addresses: pd.DataFrame, ppd_addresses: pd.DataFrame
) -> pd.DataFrame:
    return epc_addresses.merge(ppd_addresses, on="postcode", suffixes=("_epc", "_ppd"))


def add_candidate_pair_features(candidate_matches: pd.DataFrame) -> pd.DataFrame:
    return (
        candidate_matches.pipe(add_building_name_in_paon)
        .pipe(add_building_number_in_paon)
        .pipe(add_flat_number_match)
    )


def add_building_number_in_paon(candidate_matches: pd.DataFrame) -> pd.DataFrame:
    def _building_number_in_paon(
        building_number: Optional[str], paon: Optional[str]
    ) -> int:
        return 1 if building_number == paon else 0

    candidate_matches["_building_number_in_paon"] = [
        _building_number_in_paon(building_number, paon)
        for building_number, paon in zip(
            candidate_matches["building_number"],
            candidate_matches["primary_addressable_object_name"],
        )
    ]

    return candidate_matches


def add_building_name_in_paon(candidate_matches: pd.DataFrame) -> pd.DataFrame:
    def _building_name_in_paon(
        building_name: Optional[str], paon: Optional[str]
    ) -> int:
        if building_name is None or paon is None:
            return 0
        return 1 if building_name in paon else 0

    candidate_matches["_building_name_in_paon"] = [
        _building_name_in_paon(building_name, paon)
        for building_name, paon in zip(
            candidate_matches["building_name"],
            candidate_matches["primary_addressable_object_name"],
        )
    ]
    return candidate_matches


def add_flat_number_match(candidate_matches: pd.DataFrame) -> pd.DataFrame:
    # https://github.com/pandas-dev/pandas/issues/20442#issuecomment-375247686
    candidate_matches["_flat_number_match"] = (
        candidate_matches["flat_number_epc"]
        .eq(candidate_matches["flat_number_ppd"])
        .astype(int)
    )
    return candidate_matches


def add_similarity_score(candidate_matches: pd.DataFrame) -> pd.DataFrame:
    feature_columns = [col for col in candidate_matches if col.startswith("_")]
    expr = "(" + " + ".join(feature_columns) + f") / {len(feature_columns)}"
    candidate_matches["_score"] = candidate_matches.eval(expr)
    return candidate_matches
