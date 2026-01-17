from .edges import GraphEdge, START
from .nodes import GraphNode
from .states import GraphState
from typing import Type, TypeVar, Generic, Callable, Awaitable, Sequence
from collections import defaultdict
import asyncio

T = TypeVar('T', bound=GraphState)

class GraphExecutor(Generic[T]):

    edges: list[GraphEdge[T]]

    def __init__(self, edges: list[GraphEdge[T]]):
        self.edges = edges
        self._index: dict[GraphNode[T] | Type[START], list[GraphEdge[T]]] = defaultdict(list[GraphEdge[T]])

        for edge in self.edges:
            if isinstance(edge.source, list):
                for source in edge.source:
                    self._index[source].append(edge)
            else:
                self._index[edge.source].append(edge)
                

    async def __call__(self, initial_state: T) -> T:
        state: T = initial_state
        current_nodes: Sequence[GraphNode[T] | Type[START]] = [START]

        while True:

            edges: list[GraphEdge[T]] = []

            for current_node in current_nodes:

                # Find the edge corresponding to the current node
                edges.extend(self._index[current_node])


            next_nodes: list[GraphNode[T]] = [
                n for edge in edges 
                if isinstance((n := edge.next(state)), GraphNode)
            ] # Only one execution of next filtering "END"s


            if not next_nodes:
                break # END


            parallel_tasks: list[Callable[[T], Awaitable[None]]] = []
            sequential_tasks: list[Callable[[T], Awaitable[None]]] = []
            # Determine the next node using the edge's next function
            for next_node in next_nodes:
                
                parallel_tasks.append(next_node.run)
                sequential_tasks.append(next_node.seq)

            # Run parallel
            await asyncio.gather(*(task(state) for task in parallel_tasks))

            for seq in sequential_tasks:
                await seq(state)

            
            current_nodes = next_nodes


        return state