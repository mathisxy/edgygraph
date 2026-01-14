from typing import Callable, Type
from .states import GraphState
from .nodes import GraphNode


class START:
    pass

class END:
    pass


class GraphEdge:

    source: GraphNode[GraphState] | Type[START]
    next: Callable[[GraphState], GraphNode[GraphState] | Type[END]]

    def __init__(self, source: GraphNode[GraphState] | Type[START], next: Callable[[GraphState], GraphNode[GraphState] | Type[END]]):
        self.source = source
        self.next = next
