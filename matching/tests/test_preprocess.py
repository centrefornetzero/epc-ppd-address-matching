import pandas as pd

from matching.preprocess import (
    add_epc_features,
    add_ppd_features,
    casefold_epc_addresses,
    casefold_ppd_addresses,
    extract_building_name,
    extract_building_number,
    extract_flat_number,
)


def test_extract_building_name() -> None:
    assert extract_building_name(None) is None
    assert extract_building_name("wayfarer") == "wayfarer"
    assert extract_building_name("grey house") == "grey house"
    assert extract_building_name("flat a") is None
    assert extract_building_name("32 rochdale road") is None
    assert extract_building_name("32, rochdale road") is None


def test_extract_building_number() -> None:
    assert extract_building_number(None) is None
    assert extract_building_number("8 south road") == "8"
    assert extract_building_number("23, halifax road") == "23"
    assert extract_building_number("8c, letchmore road") == "8c"
    assert extract_building_number("18c meldon road") == "18c"
    assert extract_building_number("flat a") is None
    assert extract_building_number("flat 3") is None


def test_extract_flat_number() -> None:
    assert extract_flat_number("flat 15") == "15"
    assert extract_flat_number("flat 7") == "7"
    assert extract_flat_number("flat 1 queens court") == "1"
    assert extract_flat_number("flat a") == "a"
    assert extract_flat_number("basement flat") == "basement"
    assert extract_flat_number("first floor flat") == "first floor"
    assert extract_flat_number("fancy building name") is None


def test_casefold_epc_addresses() -> None:
    epc_addresses = pd.DataFrame(
        [
            {
                "address_line_1": "First Floor Flat",
                "address_line_2": "1, London Road",
                "address_line_3": "Marsden Moor",
            }
        ]
    )

    got = epc_addresses.pipe(casefold_epc_addresses)

    want = pd.DataFrame(
        [
            {
                "address_line_1": "first floor flat",
                "address_line_2": "1, london road",
                "address_line_3": "marsden moor",
            }
        ]
    )

    pd.testing.assert_frame_equal(got, want)


def test_add_epc_features() -> None:
    tests = [
        (
            {
                "address_line_1": "flat c",
                "address_line_2": "97, grove road",
            },
            {"building_number": "97", "flat_number": "c"},
        ),
        (
            {
                "address_line_1": "32, pilkington road",
                "address_line_2": None,
            },
            {"building_number": "32"},
        ),
        (
            {
                "address_line_1": "the mansion",
                "address_line_2": "1, garden road",
            },
            {"building_name": "the mansion", "building_number": "1"},
        ),
    ]

    addresses, features = zip(*tests)
    got = pd.DataFrame(addresses).pipe(add_epc_features)[
        ["building_number", "flat_number", "building_name"]
    ]
    want = pd.DataFrame(features)
    pd.testing.assert_frame_equal(got, want)


def test_casefold_ppd_addresses() -> None:
    ppd_addresses = pd.DataFrame(
        [
            {
                "primary_addressable_object_name": "6a",
                "secondary_addressable_object_name": "LOWER MAISONETTE",
                "street": "SURREY AVENUE",
            }
        ]
    )

    got = ppd_addresses.pipe(casefold_ppd_addresses)

    want = pd.DataFrame(
        [
            {
                "primary_addressable_object_name": "6a",
                "secondary_addressable_object_name": "lower maisonette",
                "street": "surrey avenue",
            }
        ]
    )

    pd.testing.assert_frame_equal(got, want)


def test_add_ppd_features() -> None:
    tests = [
        (
            {
                "primary_addressable_object_name": "21",
                "secondary_addressable_object_name": "flat 4",
            },
            {"flat_number": "4"},
        ),
    ]

    addresses, features = zip(*tests)
    got = pd.DataFrame(addresses).pipe(add_ppd_features)[["flat_number"]]
    want = pd.DataFrame(features)
    pd.testing.assert_frame_equal(got, want)
