import argparse
import textwrap
from typing import Optional, Sequence, cast

import farmhash
import pyarrow as pa
import pyarrow.parquet as pq


def get_args(args: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=textwrap.dedent(
            """
    Partition or re-partition a Parquet dataset into a specified number of partitions.

    Records are partitioned using the hash of the specified column, so two different
    files can be split into equivalent partitions if they share the same column values.
    """
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("input_path", help="Path to Parquet file(s)")
    parser.add_argument("output_path", help="Path to write partitioned Parquest files")
    parser.add_argument(
        "-n",
        "--num_partitions",
        default=100,
        type=int,
        help="Number of partitions (default: %(default)s)",
    )
    parser.add_argument(
        "-c",
        "--partition_col",
        default="postcode",
        help="Column on which to partition data (default: %(default)s)",
    )
    return parser.parse_args(args)


def partition_data(
    input_path: str, output_path: str, num_partitions: int, partition_col: str
) -> None:
    def partition_key(value: str) -> int:
        hashed_value = cast(int, farmhash.hash64(value))
        return abs(hashed_value) % num_partitions

    input_table = pq.read_table(input_path)
    partition_keys = pa.array(
        partition_key(str(value)) for value in input_table[partition_col]
    )
    input_table = input_table.append_column("partition_key", partition_keys)
    pq.write_to_dataset(input_table, output_path, partition_cols=["partition_key"])


if __name__ == "__main__":
    args = get_args()
    partition_data(**vars(args))
