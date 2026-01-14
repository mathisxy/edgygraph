from .edges import GraphEdge, START, END
from .nodes import GraphNode
from .states import GraphState
from typing import Type


class GraphExecutor:

    edges: list[GraphEdge]

    def __init__(self, edges: list[GraphEdge]):
        self.edges = edges

    async def __call__(self, initial_state: GraphState) -> GraphState:
        state = initial_state
        current_node: GraphNode[GraphState] | Type[START] = START

        index_dict: dict[GraphNode[GraphState] | Type[START], GraphEdge] = {edge.source: edge for edge in self.edges}

        while True:
            # Find the edge corresponding to the current node
            edge: GraphEdge = index_dict[current_node]
            # Determine the next node using the edge's next function
            next_node = edge.next(state)

            if next_node == END:
                break
            else:
                assert isinstance(next_node, GraphNode)
                # Run the current node to update the state
                state: GraphState = await next_node.run(state)
                current_node = next_node

        return state