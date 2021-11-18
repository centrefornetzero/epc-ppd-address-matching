from typing import Iterator, Set

import networkx as nx
import pandas as pd


def get_matches(
    candidate_matches: pd.DataFrame, min_score: float
) -> Iterator[Set[str]]:
    min_similarity = candidate_matches["_score"] >= min_score
    edges = (
        tuple(edge)
        for edge in candidate_matches[min_similarity][["epc_id", "ppd_id"]].itertuples(
            index=False
        )
    )
    similarity_graph = nx.Graph()
    similarity_graph.add_edges_from(edges)
    yield from nx.connected_components(similarity_graph)
