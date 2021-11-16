import pandas as pd


def get_candidate_pairs(
    epc_addresses: pd.DataFrame, ppd_addresses: pd.DataFrame
) -> pd.DataFrame:
    return epc_addresses.merge(ppd_addresses, on="postcode", suffixes=("_epc", "_ppd"))
