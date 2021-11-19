from typing import Iterable, Iterator, Set

import networkx as nx
import pandas as pd


def get_matches(
    candidate_matches: pd.DataFrame, min_score: float
) -> Iterator[Set[str]]:
    min_similarity = candidate_matches["_score"] >= min_score
    edges = zip(
        candidate_matches.loc[min_similarity, "epc_id"],
        candidate_matches.loc[min_similarity, "ppd_id"],
    )
    similarity_graph = nx.Graph()
    similarity_graph.add_edges_from(edges)
    yield from nx.connected_components(similarity_graph)


def tabulate_matches(matches: Iterable[Set[str]]) -> pd.Series:
    tabulated_matches = []
    for cluster_id, nodes in enumerate(matches):
        tabulated_matches.extend([(node, cluster_id) for node in nodes])

    return pd.DataFrame(
        tabulated_matches, columns=["address_id", "cluster_id"]
    ).set_index("address_id")["cluster_id"]
