import pandas as pd

from matching.matches import get_matches, prefix_cluster_ids, tabulate_matches


def test_get_matches() -> None:
    candidates = pd.DataFrame(
        {
            "epc_id": ["epc_1", "epc_1", "epc_1", "epc_2"],
            "ppd_id": ["ppd_1", "ppd_2", "ppd_3", "ppd_4"],
            "_score": [0, 0.5, 1, 1],
        }
    )
    matches = list(get_matches(candidates, min_score=0.5))
    assert matches == [{"epc_1", "ppd_2", "ppd_3"}, {"epc_2", "ppd_4"}]


def test_tabulate_matches() -> None:
    matches = [{"epc_1", "ppd_1"}, {"epc_2", "epc_3", "ppd_2"}]
    got = tabulate_matches(matches)
    want = pd.Series(
        [0, 0, 1, 1, 1],
        pd.Index(["epc_1", "ppd_1", "epc_2", "epc_3", "ppd_2"], name="address_id"),
        name="cluster_id",
    )
    pd.testing.assert_series_equal(got.sort_index(), want.sort_index())


def test_prefix_cluster_ids() -> None:
    index = pd.Index(["epc_1", "ppd_1", "epc_2", "ppd_2"])
    tabulated_matches = pd.Series([0, 0, 1, 1], index)
    got = tabulated_matches.pipe(prefix_cluster_ids, "prefix-")
    want = pd.Series(
        ["prefix-0", "prefix-0", "prefix-1", "prefix-1"], index, dtype=pd.StringDtype()
    )
    pd.testing.assert_series_equal(got, want)


def test_skip_prefix_cluster_ids_when_prefix_is_none() -> None:
    tabulated_matches = pd.Series(
        [0, 0, 1, 1], pd.Index(["epc_1", "ppd_1", "epc_2", "ppd_2"])
    )
    pd.testing.assert_series_equal(
        tabulated_matches.pipe(prefix_cluster_ids, None), tabulated_matches
    )
