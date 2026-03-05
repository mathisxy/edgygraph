from __future__ import annotations
from typing import Hashable, cast
from collections import defaultdict
from collections.abc import Hashable
import asyncio

from ..states import StateProtocol, SharedProtocol
from ..diff import Change
from .types import Edge, ErrorEdge, Entry, ErrorEntry, SingleSource, SingleErrorSource, Config, ErrorConfig, Types, SingleSourceBranchContainer


class Branch[T: StateProtocol, S: SharedProtocol]:
    """
    A branch of the graph.


    Args:
        edges: The edges of the branch.
        join: The node to join the branch after execution. If None the branch will not be joined.

    """


    def __init__(self, edges: SingleSourceBranchContainer[T, S]) -> None:

        self.edges = edges[:-1]
        self.source = edges[0]
        self.join = edges[-1]

        self.result: asyncio.Future[dict[tuple[Hashable, ...], Change]] | None = None

        self.edge_index: dict[SingleSource[T, S], list[Entry[T, S]]] = defaultdict(list)
        self.error_edge_index: dict[SingleErrorSource[T, S], list[ErrorEntry[T, S]]] = defaultdict(list)

        self.index_edges()


    def index_edges(self) -> None:
        """
        Index the edges by single source.

        Append the edges to
        - `edge_index` if the edge is a normal edge
        - `error_edge_index` if the edge is an error edge
        """

        # for i, edge in enumerate(self.edges):


        for i, (source, next) in enumerate(zip(self.edges, [*self.edges[1:], False])):

            if Types[T, S].is_any_edge(source):
                edge = source

            elif Types[T, S].is_any_single_source(source) and Types[T, S].is_next(next):
                edge = (source, next)

            elif Types[T, S].is_next(next) and not Types[T, S].is_any_single_source(next): # is next but not single source -> cannot be reached and cannot be a source
                raise ValueError(f"Next is never reached: source: {source}, next: {next} in branch with source {self.source} and join {self.join}")
            
            else:
                continue # Next meets source, skip
                
            self.index_edge(cast(Edge[T, S] | ErrorEdge[T, S], edge), i)
        
            
    def index_edge(self, edge: Edge[T, S] | ErrorEdge[T, S], index: int) -> None:
        """
        Index a single edge by its source.

        Append the edge to
        - `edge_index` if the edge is a normal edge
        - `error_edge_index` if the edge is an error edge

        Args:
            edge: The edge to index.
            index: The original index of the edge in the list of edges of the branch.
        """

        print(f"Indexing edge {edge} at index {index} in branch with source {self.source} and join {self.join}")

        match edge:
            case (source, next, config): pass
            case (source, next): config = None
            case _: raise ValueError(f"Invalid edge format: {edge}")
        
        if Types[T, S].is_error_source(source):
            config = config or ErrorConfig()
            assert isinstance(config, ErrorConfig), f"Unexpected properties type for error edge {edge}: {config}"

            if Types[T, S].is_single_error_source(source):
                self.error_edge_index[source].append(ErrorEntry[T, S](next=next, config=config, index=index))
            elif Types[T, S].is_single_error_source_list(source):
                for single_error_source in source:
                    self.error_edge_index[single_error_source].append(ErrorEntry[T, S](next=next, config=config, index=index))
            else:
                raise ValueError(f"Invalid error source: {source}")
            
        elif Types[T, S].is_source(source):
            config = config or Config()
            assert isinstance(config, Config), f"Unexpected properties type for node edge {edge}: {config}"

            if Types[T, S].is_single_source(source):
                self.edge_index[source].append(Entry[T, S](next=next, config=config, index=index))
            elif Types[T, S].is_single_source_list(source):
                for single_source in source:
                    self.edge_index[single_source].append(Entry[T, S](next=next, config=config, index=index))
            else:
                raise ValueError(f"Invalid source: {source}")

        else:
            raise ValueError(f"Invalid edge source: {edge[0]}")