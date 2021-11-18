from matching.__main__ import get_args


def test_get_args() -> None:
    args = get_args(["epc_path", "ppd_path", "output_path"])
    assert args.epc_path == "epc_path"
    assert args.ppd_path == "ppd_path"
    assert args.output_path == "output_path"
