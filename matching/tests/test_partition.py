import os
import pathlib

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from matching.partition import get_args, partition_data


def test_get_args() -> None:
    args = get_args(["input.parquet", "output_path"])
    assert args.input_path == "input.parquet"
    assert args.output_path == "output_path"


def test_partition_data(tmp_path: pathlib.Path) -> None:
    table = pa.Table.from_pydict(
        {
            "id": [1, 2, 3, 4],
            "postcode": ["HG2 9LW", "HG2 9LW", "CF31 3PD", "RM14 3HR"],
        }
    )
    input_path = str(tmp_path / "input.parquet")
    pq.write_table(table, input_path)

    output_path = str(tmp_path / "output")
    num_partitions = 2
    partition_data(input_path, output_path, num_partitions, "postcode")

    partitioned_dataset = pq.ParquetDataset(output_path)
    assert len(os.listdir(output_path)) == num_partitions

    got = (
        partitioned_dataset.read()
        .to_pandas()
        .drop("partition_key", axis=1)
        .sort_values(by="id")
        .reset_index(drop=True)
    )
    want = table.to_pandas().sort_values(by="id").reset_index(drop=True)
    pd.testing.assert_frame_equal(got, want)
