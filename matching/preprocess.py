import re
from typing import Optional

import pandas as pd


def extract_building_name(address_line: Optional[str]) -> Optional[str]:
    if address_line is None or "flat" in address_line:
        return None

    if match := re.match(r"[a-z\s]+", address_line):
        return match.group()

    return None


def extract_building_number(address_line: Optional[str]) -> Optional[str]:
    if address_line is None:
        return None

    if match := re.match(r"[0-9]+[a-z]*", address_line):
        return match.group()

    return None


def extract_flat_number(address_line: str) -> Optional[str]:
    if address_line is None:
        return None

    if match := re.match(r"flat ([a-z0-9]+)", address_line):
        return match.group(1)

    if match := re.match(r"([\w\s]+)(?=\sflat)", address_line):
        return match.group(1)

    return None


def casefold_epc_addresses(addresses: pd.DataFrame) -> pd.DataFrame:
    for col in ["address_line_1", "address_line_2", "address_line_3"]:
        addresses[col] = addresses[col].str.casefold()
    return addresses


def add_epc_features(addresses: pd.DataFrame) -> pd.DataFrame:
    building_number_address_line_1 = addresses["address_line_1"].map(
        extract_building_number
    )
    building_number_address_line_2 = addresses["address_line_2"].map(
        extract_building_number
    )
    addresses["building_number"] = building_number_address_line_1.combine_first(
        building_number_address_line_2
    )
    addresses["flat_number"] = addresses["address_line_1"].map(extract_flat_number)
    addresses["building_name"] = addresses["address_line_1"].map(extract_building_name)
    return addresses


def casefold_ppd_addresses(addresses: pd.DataFrame) -> pd.DataFrame:
    for col in [
        "primary_addressable_object_name",
        "secondary_addressable_object_name",
        "street",
    ]:
        addresses[col] = addresses[col].str.casefold()
    return addresses


def add_ppd_features(addresses: pd.DataFrame) -> pd.DataFrame:
    addresses["flat_number"] = addresses["secondary_addressable_object_name"].map(
        extract_flat_number
    )
    return addresses
