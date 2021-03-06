import pandas as pd

from matching.candidates import (
    add_building_name_in_paon,
    add_building_number_in_paon,
    add_flat_number_match,
    add_similarity_score,
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
            1,
        ),
        (
            {
                "building_number": "1",
                "primary_addressable_object_name": "11",
            },
            0,
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
            1,
        ),
        (
            {
                "building_name": "cottage",
                "primary_addressable_object_name": "32",
            },
            0,
        ),
        (
            {
                "building_name": None,
                "primary_addressable_object_name": "32",
            },
            0,
        ),
        (
            {
                "building_name": "windsor palace",
                "primary_addressable_object_name": None,
            },
            0,
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
            1,
        ),
        (
            {"flat_number_epc": "1", "flat_number_ppd": "11"},
            0,
        ),
        (
            {"flat_number_epc": None, "flat_number_ppd": "1"},
            0,
        ),
        (
            {"flat_number_epc": None, "flat_number_ppd": None},
            1,
        ),
    ]
    candidates, building_number_in_paon = zip(*tests)
    got = pd.DataFrame(candidates).pipe(add_flat_number_match)["_flat_number_match"]
    want = pd.Series(building_number_in_paon)
    pd.testing.assert_series_equal(got, want, check_names=False)


def test_add_similarity_score() -> None:
    candidate_matches = pd.DataFrame(
        {
            "_feat_1": [1, 0, 0],
            "_feat_2": [1, 1, 0],
        }
    )
    got = candidate_matches.pipe(add_similarity_score)["_score"]
    want = pd.Series([1.0, 0.5, 0.0])
    pd.testing.assert_series_equal(got, want, check_names=False)
