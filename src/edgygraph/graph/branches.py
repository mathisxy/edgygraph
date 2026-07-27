from __future__ import annotations
from typing import Hashable
from collections import defaultdict
from collections.abc import Hashable
import asyncio

from ..states import StateProtocol, SharedProtocol
from ..diff import Change
from .types import Edge, ErrorEdge, Entry, ErrorEntry, ErrorSource, Flexible, UnresolvedSource, UnresolvedNext, Types, BranchContainer, Source, Next, NextCallable


class Branch[T: StateProtocol, S: SharedProtocol]:
    """
    A branch of the graph.

    The join parameter is extracted from the last element of the edges.


    Args:
        edges: The edges of the branch.
        source: The source of the branch, which is handled as start node of the branch.

    """


    def __init__(self, edges: BranchContainer[T, S], source: Source[T, S]) -> None:

        self.edges = edges[:-1]
        self.source = source
        self.join = edges[-1]

        self.result: asyncio.Future[dict[tuple[Hashable, ...], Change]] | None = None

        self.edge_index: dict[Source[T, S], list[Entry[T, S]]] = defaultdict(list)
        self.error_edge_index: dict[ErrorSource[T, S], list[ErrorEntry[T, S]]] = defaultdict(list)

        self.index_edges()


    def index_edges(self) -> None:
        """
        Index the edges by single source.
        """


        for i, (source, next) in enumerate(zip(self.edges, self.edges[1:])):

            if not Types[T, S].is_unresolved_source(source) and not Types[T, S].is_unresolved_next(next):
                raise ValueError(f"Invalid edge: source: {source}, next: {next} in branch with source {self.source} and join {self.join}")

            if Types[T, S].is_unresolved_source(source) and Types[T, S].is_unresolved_next(next):
                
                try:

                    filtered_source = self.filter_source_by_config(source)
                    filtered_next = self.filter_next_by_config(next)

                except EmptyFilterResult:

                    if Types[T, S].is_unresolved_source(next) and Types[T, S].is_unresolved_next(source):
                        try:
                            self.filter_source_by_config(next)
                            self.filter_next_by_config(source)
                            continue
                        except EmptyFilterResult:
                            raise ValueError(f"No edge after filtering and source is empty after filtering as next or next is empty after filtering as source: source: {source}, next: {next} in branch with source {self.source} and join {self.join}")
                    else:
                        raise ValueError(f"No edge after filtering and source is not a valid next and next is not a valid source: source: {source}, next: {next} in branch with source {self.source} and join {self.join}")


                sources = filtered_source if isinstance(filtered_source, list) else [filtered_source]

                for s in sources:

                    if Types[T, S].is_source(s):
                        self.index_edge(Edge(source=s, next=filtered_next), i)
                    elif Types[T, S].is_error_source(s):
                        self.index_edge(ErrorEdge(source=s, next=filtered_next), i)
                    else:
                        raise ValueError(f"Invalid filtered source: {filtered_source} from original source: {source} in branch with source {self.source} and join {self.join}")
            
                        
            
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
            case ErrorEdge(source=source, next=next):
                self.error_edge_index[source].append(ErrorEntry[T, S](next=next, index=index))
            case Edge(source=source, next=next):
                self.edge_index[source].append(Entry[T, S](next=next, index=index))
        


    def filter_source_by_config(self, source: Flexible[UnresolvedSource[T, S]]) -> list[Source[T, S]] | ErrorSource[T, S]:
        """
        Filter the source by its config.

        The config is used to track the Operators `-` and `+` on nodes.

        Args:
            source: The source to filter.

        Returns:
            The filtered source without config.

        Raises:
            EmptyFilterResult: If the whole source is filtered out.
        """

        if Types[T, S].is_error_source(source):
            return source # Error sources have no filters
        
        if Types[T, S].is_unresolved_source_only_flexible(source):

            source_list = source if isinstance(source, list) else [source]
            filtered_sources: list[Source[T, S]] = []

            for single_source in source_list:

                if Types[T, S].is_node_with_config(single_source):

                    (config, single_source) = single_source

                    if not config.only_next:
                        filtered_sources.append(single_source)

                elif Types[T, S].is_source(single_source):
                    filtered_sources.append(single_source)
                else:
                    raise ValueError(f"Invalid source: {single_source} in branch with source {self.source} and join {self.join}")
                
            if filtered_sources == []:
                raise EmptyFilterResult()
            
            return filtered_sources
        
        else:
            raise ValueError(f"Invalid source: {source} in branch with source {self.source} and join {self.join}")



    def filter_next_by_config(self, next: UnresolvedNext[T, S]) -> list[Next[T, S]] | NextCallable[T, S]:
        """
        Filter the next by its config.

        The config is used to track the Operators `-` and `+` on nodes.

        Args:
            next: The next to filter.
        
        Returns:
            The filtered next without the config.

        Raises:
            EmptyFilterResult: If the next is filtered out.
        """

        if Types[T, S].is_next_callable(next):
            return next
        
        if Types[T, S].is_unresolved_next_only_flexible(next):

            if not isinstance(next, list):
                next_list = [next]
            else:
                next_list = next
            
            filtered_next: list[Next[T, S]] = []

            for single_next in next_list:

                if isinstance(single_next, tuple):

                    (config, single_next) = single_next

                    if not config.only_source:
                        filtered_next.append(single_next)

                else:
                    filtered_next.append(single_next)

            if filtered_next == []:
                raise EmptyFilterResult()
            
            return filtered_next
        
        else:
            raise ValueError(f"Invalid next: {next} in branch with source {self.source} and join {self.join}")



class EmptyFilterResult(Exception):
    """
    Exception raised when a filter result is empty.
    """