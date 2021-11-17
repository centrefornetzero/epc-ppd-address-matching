from typing import Optional, cast

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
    ) -> bool:
        return building_number == paon

    candidate_matches["_building_number_in_paon"] = [
        _building_number_in_paon(building_number, paon)
        for building_number, paon in zip(
            candidate_matches["building_number"],
            candidate_matches["primary_addressable_object_name"],
        )
    ]

    return candidate_matches


def add_building_name_in_paon(candidate_matches: pd.DataFrame) -> pd.DataFrame:
    def _building_name_in_paon(building_name: Optional[str], paon: str) -> bool:
        if pd.isnull(building_name):
            return False
        return cast(str, building_name) in paon

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
    candidate_matches["_flat_number_match"] = candidate_matches["flat_number_epc"].eq(
        candidate_matches["flat_number_ppd"]
    )
    return candidate_matches
