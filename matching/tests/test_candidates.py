import pandas as pd

from matching.candidates import get_candidate_pairs


def test_get_candidate_pairs():
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
