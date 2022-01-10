import pathlib
import subprocess
from typing import Dict, List, Set

import pandas as pd
import pytest

tests = [
    # simple building number match
    (
        [
            {
                "epc_id": "epc_1",
                "address_line_1": "15, Carrot Road",
                "postcode": "BN21 2JG",
            },
            {
                "epc_id": "epc_2",
                "address_line_1": "16, Carrot Road",
                "postcode": "BN21 2JG",
            },
        ],
        [
            {
                "ppd_id": "ppd_1",
                "primary_addressable_object_name": "14",
                "street": "Carrot Road",
                "postcode": "BN21 2JG",
            },
            {
                "ppd_id": "ppd_2",
                "primary_addressable_object_name": "15",
                "street": "Carrot Road",
                "postcode": "BN21 2JG",
            },
        ],
        [
            {"epc_1", "ppd_2"},
        ],
    ),
    # Flat number and building number
    (
        [
            {
                "epc_id": "epc_1",
                "address_line_1": "Flat 1",
                "address_line_2": "76, Dimbolton Road",
                "postcode": "MK40 2NZ",
            },
        ],
        [
            {
                "ppd_id": "ppd_1",
                "primary_addressable_object_name": "76",
                "secondary_addressable_object_name": "Flat 1",
                "postcode": "MK40 2NZ",
            }
        ],
        [{"epc_1", "ppd_1"}],
    ),
    # Apartment number and building number
    (
        [
            {
                "epc_id": "epc_apartment_1234",
                "address_line_1": "Apartment 1234",
                "address_line_2": "34, Greenfield Road",
                "address_line_3": "Joyce Green",
                "postcode": "WA14 4RP",
            }
        ],
        [
            {
                "ppd_id": "ppd_apartment_1234",
                "primary_addressable_object_name": "34",
                "secondary_addressable_object_name": "Apartment 1234",
                "postcode": "WA14 4RP",
            },
            {
                "ppd_id": "ppd_apartment_5678",
                "primary_addressable_object_name": "34",
                "secondary_addressable_object_name": "Apartment 5678",
                "postcode": "WA14 4RP",
            },
        ],
        [{"epc_apartment_1234", "ppd_apartment_1234"}],
    ),
    # Building name and no number
    (
        [
            {
                "epc_id": "epc_1",
                "address_line_1": "Firlogs",
                "address_line_2": "Edwards Drive",
                "postcode": "HR2 7AB",
            },
            {
                "epc_id": "epc_2",
                "address_line_1": "Aingarth",
                "address_line_2": "Edwards Drive",
                "postcode": "HR2 7AB",
            },
        ],
        [
            {
                "ppd_id": "ppd_1",
                "primary_addressable_object_name": "Firlogs",
                "postcode": "HR2 7AB",
            },
        ],
        [{"epc_1", "ppd_1"}],
    ),
    # Flat and building name in address lines 1 and 2 of EPC
    (
        [
            {
                "epc_id": "epc_1",
                "address_line_1": "Flat 100",
                "address_line_2": "Henry Palace Mansions",
                "address_line_3": "Spring Gardens",
                "postcode": "SW11 4DX",
            },
            {
                "epc_id": "epc_2",
                "address_line_1": "Flat 101",
                "address_line_2": "Henry Palace Mansions",
                "address_line_3": "Spring Gardens",
                "postcode": "SW11 4DX",
            },
        ],
        [
            {
                "ppd_id": "ppd_1",
                "primary_addressable_object_name": "Henry Palace Mansions",
                "secondary_addressable_object_name": "Flat 100",
                "street": "Spring Gardens",
                "postcode": "SW11 4DX",
            },
            {
                "ppd_id": "ppd_2",
                "primary_addressable_object_name": "Henry Palace Mansions",
                "secondary_addressable_object_name": "Flat 101",
                "street": "Spring Gardens",
                "postcode": "SW11 4DX",
            },
        ],
        [
            {"epc_1", "ppd_1"},
            {"epc_2", "ppd_2"},
        ],
    ),
]


@pytest.mark.parametrize("epc_addresses, ppd_addresses, want", tests)
def test_matching(
    epc_addresses: List[Dict[str, str]],
    ppd_addresses: List[Dict[str, str]],
    want: List[Set[str]],
    tmpdir: pathlib.Path,
) -> None:
    epc_path = str(tmpdir / "epc_addresses.parquet")

    pd.DataFrame(
        epc_addresses,
        columns=[
            "epc_id",
            "address_line_1",
            "address_line_2",
            "address_line_3",
            "postcode",
        ],
        dtype=str,
    ).to_parquet(epc_path)

    ppd_path = str(tmpdir / "ppd_addresses.parquet")

    pd.DataFrame(
        ppd_addresses,
        columns=[
            "ppd_id",
            "primary_addressable_object_name",
            "secondary_addressable_object_name",
            "street",
            "postcode",
        ],
        dtype=str,
    ).to_parquet(ppd_path)

    output_path = str(tmpdir / "matches.parquet")

    subprocess.run(
        ["python", "-m", "matching", epc_path, ppd_path, output_path], check=True
    )

    got = (
        pd.read_parquet(output_path)
        .groupby("cluster_id")["address_id"]
        .apply(set)
        .to_list()
    )

    assert got == want
