from __future__ import annotations
from typing import Hashable
from collections import defaultdict
from collections.abc import Hashable, Sequence
import asyncio

from ..nodes import START, END, Node
from ..states import StateProtocol, SharedProtocol
from ..diff import Change
from .types import NodeTupel, Edge, ErrorEdge, SingleNext, Entry, ErrorEntry, SingleSource, SingleErrorSource, Config, ErrorConfig, Types


class Branch[T: StateProtocol, S: SharedProtocol]:
    """
    A branch of the graph.


    Args:
        edges: The edges of the branch.
        join: The node to join the branch after execution. If None the branch will not be joined.

    """


    def __init__(self, edges: Sequence[Edge[T, S] | ErrorEdge[T, S] | NodeTupel[T, S]], start: SingleSource[T, S], join: SingleNext[T, S] = None) -> None:

        self.edges = edges
        self.start = start
        self.join = join

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

        for i, edge in enumerate(self.edges):

            # Node Sequence
            if Types[T, S].is_node_tupel(edge):

                for source, next in zip(edge, edge[1:]):
                    if isinstance(source, type): assert source is START, f"Unexpected type in node sequence: {source}"
                    if isinstance(next, type): assert next is END, f"Unexpected type in node sequence: {next}"
                    assert isinstance(source, (Node, type)), f"Unexpected source type in node sequence: {source}"
                    self.edge_index[source].append(Entry[T, S](next=next, config=Config(), index=i))
                    
                continue

            match edge:
                case (source, next, config): pass
                case (source, next): config = None
                case _: raise ValueError(f"Invalid edge format: {edge}")
            
            if Types[T, S].is_error_source(source):
                config = config or ErrorConfig()
                assert isinstance(config, ErrorConfig), f"Unexpected properties type for error edge {edge}: {config}"

                if Types[T, S].is_single_error_source(source):
                    self.error_edge_index[source].append(ErrorEntry[T, S](next=next, config=config, index=i))
                elif Types[T, S].is_single_error_source_sequence(source):
                    for single_error_source in source:
                        self.error_edge_index[single_error_source].append(ErrorEntry[T, S](next=next, config=config, index=i))
                else:
                    raise ValueError(f"Invalid error source: {source}")
                
            elif Types[T, S].is_source(source):
                config = config or Config()
                assert isinstance(config, Config), f"Unexpected properties type for node edge {edge}: {config}"

                if Types[T, S].is_single_source(source):
                    self.edge_index[source].append(Entry[T, S](next=next, config=config, index=i))
                elif Types[T, S].is_single_source_sequence(source):
                    for single_source in source:
                        self.edge_index[single_source].append(Entry[T, S](next=next, config=config, index=i))
                else:
                    raise ValueError(f"Invalid source: {source}")

            else:
                raise ValueError(f"Invalid edge source: {edge[0]}")