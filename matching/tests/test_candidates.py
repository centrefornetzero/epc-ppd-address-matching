import pandas as pd

from matching.candidates import (
    add_building_name_in_paon,
    add_building_number_in_paon,
    add_flat_number_match,
    get_candidate_pairs,
)


def test_get_candidate_pairs() -> None:
    epc_addresses = pd.DataFrame(
        [
            {"epc_id": 1, "flat_number": "1", "postcode": "E1 1AA"},
            {"epc_id": 2, "flat_number": "2", "postcode": "E1 1AA"},
            {"epc_id": 3, "flat_number": "3", "postcode": "SW1 1AA"},
        ]
    )
    ppd_addresses = pd.DataFrame(
        [
            {"ppd_id": 1, "flat_number": "1", "postcode": "E1 1AA"},
            {"ppd_id": 2, "flat_number": "2", "postcode": "SW1 1AA"},
            {"ppd_id": 3, "flat_number": "3", "postcode": "SW1 1AA"},
        ]
    )

    got = get_candidate_pairs(epc_addresses, ppd_addresses)
    want = pd.DataFrame(
        [
            {
                "epc_id": 1,
                "postcode": "E1 1AA",
                "ppd_id": 1,
                "flat_number_epc": "1",
                "flat_number_ppd": "1",
            },
            {
                "epc_id": 2,
                "postcode": "E1 1AA",
                "ppd_id": 1,
                "flat_number_epc": "2",
                "flat_number_ppd": "1",
            },
            {
                "epc_id": 3,
                "postcode": "SW1 1AA",
                "ppd_id": 2,
                "flat_number_epc": "3",
                "flat_number_ppd": "2",
            },
            {
                "epc_id": 3,
                "postcode": "SW1 1AA",
                "ppd_id": 3,
                "flat_number_epc": "3",
                "flat_number_ppd": "3",
            },
        ]
    )
    pd.testing.assert_frame_equal(got, want, check_like=True)


def test_add_building_number_in_paon() -> None:
    tests = [
        (
            {
                "building_number": "1",
                "primary_addressable_object_name": "1",
            },
            True,
        ),
        (
            {
                "building_number": "1",
                "primary_addressable_object_name": "11",
            },
            False,
        ),
    ]
    candidates, building_number_in_paon = zip(*tests)
    got = pd.DataFrame(candidates).pipe(add_building_number_in_paon)[
        "_building_number_in_paon"
    ]
    want = pd.Series(building_number_in_paon)
    pd.testing.assert_series_equal(got, want, check_names=False)


def test_add_building_name_in_paon() -> None:
    tests = [
        (
            {
                "building_name": "treetops",
                "primary_addressable_object_name": "treetops",
            },
            True,
        ),
        (
            {
                "building_name": "cottage",
                "primary_addressable_object_name": "32",
            },
            False,
        ),
        (
            {
                "building_number": "1",
                "primary_addressable_object_name": "32",
            },
            False,
        ),
    ]
    candidates, building_name_in_paon = zip(*tests)
    got = pd.DataFrame(candidates).pipe(add_building_name_in_paon)[
        "_building_name_in_paon"
    ]
    want = pd.Series(building_name_in_paon)
    pd.testing.assert_series_equal(got, want, check_names=False)


def test_add_flat_number_match() -> None:
    tests = [
        (
            {"flat_number_epc": "1", "flat_number_ppd": "1"},
            True,
        ),
        (
            {"flat_number_epc": "1", "flat_number_ppd": "11"},
            False,
        ),
        (
            {"flat_number_epc": None, "flat_number_ppd": "1"},
            False,
        ),
        (
            {"flat_number_epc": None, "flat_number_ppd": None},
            True,
        ),
    ]
    candidates, building_number_in_paon = zip(*tests)
    got = pd.DataFrame(candidates).pipe(add_flat_number_match)["_flat_number_match"]
    want = pd.Series(building_number_in_paon)
    pd.testing.assert_series_equal(got, want, check_names=False)
