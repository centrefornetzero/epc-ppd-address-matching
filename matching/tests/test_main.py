from matching.__main__ import get_args


def test_get_args() -> None:
    args = get_args(["epc_path", "ppd_path", "output_path"])
    assert args.epc_path == "epc_path"
    assert args.ppd_path == "ppd_path"
    assert args.output_path == "output_path"
    assert args.cluster_prefix is None


def test_get_args_with_cluster_prefix() -> None:
    args = get_args(["--cluster-prefix", "1", "epc_path", "ppd_path", "output_path"])
    assert args.epc_path == "epc_path"
    assert args.ppd_path == "ppd_path"
    assert args.output_path == "output_path"
    assert args.cluster_prefix == "1"
