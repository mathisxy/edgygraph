from .nodes import START, END, Node
from .states import StateProtocol as State, SharedProtocol as Shared
from .graph_hooks import GraphHook
from .diff import Change, ChangeConflictException, Diff

from typing import Type, Tuple, Callable, Awaitable, cast, Any
from collections import defaultdict
from collections.abc import Hashable
import asyncio
from pydantic import BaseModel, ConfigDict, Field
import inspect


class Properties(BaseModel):

    instant: bool = False


type SourceType[T: State, S: Shared] = Node[T, S] | type[START] | list[Node[T, S] | type[START]] | type[Exception] | tuple[Node[T, S], type[Exception]] | tuple[list[Node[T, S]], type[Exception]]
type NextType[T: State, S: Shared] = Node[T, S] | type[END] | Callable[[T, S], Node[T, S] | Type[END] | Awaitable[Node[T, S] | Type[END]]]
type Edge[T: State, S: Shared] = tuple[SourceType[T, S], NextType[T, S]] | tuple[SourceType[T, S], NextType[T, S], Properties]

type EdgeIndex[T: State, S: Shared] = Node[T, S] | type[START]
type ErrorEdgeIndex[T: State, S: Shared] = type[Exception] | tuple[Node[T, S], type[Exception]]

class Graph[T: State = State, S: Shared = Shared](BaseModel):
    """
    Create and execute a graph defined by a list of edges.

    Set the required State and Shared classes via the Generic Typing Parameters.
    Because of variance its possible to use nodes, that use more general State and Shared classes (ancestors) as the Generic Typing Parameters.
    For the more flexible duck typing approch, that scales easier, use StateProtocol and SharedProtocol as Generic Typing Parameters.

    The edges are defined as a list of tuples, where the first element is the source node and the second element reveals the next node.

    Attributes:
        edges: A list of edges of compatible nodes that build the graph
        instant_edges: A list of edges of compatible nodes that run parallel to there source node
        error_edges: A list of edges of compatible nodes that run if the source node raises an exception
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    edges: list[Edge[T, S]] = Field(default_factory=list[Edge[T, S]])
    # instant_edges: list[Edge[T, S]] = Field(default_factory=list[Edge[T, S]])

    _edge_index: dict[EdgeIndex[T, S], list[NextType[T, S]]] = defaultdict(list)
    _instant_edge_index: dict[EdgeIndex[T, S], list[NextType[T, S]]] = defaultdict(list)
    _error_edge_index: dict[ErrorEdgeIndex[T, S], list[NextType[T, S]]] = defaultdict(list)

    hooks: list[GraphHook[T, S]] = Field(default_factory=list[GraphHook[T, S]], exclude=True)


    def model_post_init(self, _) -> None:
        """
        Index the edges by source node
        """

        self.index_edges(self.edges)

        

    def index_edges(self, edges: list[Edge[T, S]]) -> None:
        """
        Index the edges by source node.

        Append the edges to
            - `_edge_index` if the edge has instant set to False in properties (default value)
            - `_instant_edge_index` if the edge has instant set to True in properties
            - `_error_edge_index` if the edge is an error edge with `type[Exception]` or `tuple[Node[T, S], type[Exception]]` as source

        Args:
           edges: The edges to index
        """

        for edge in edges:

            match edge:
                case (source, next, properties): pass
                case (source, next): properties = Properties()
                case _: raise ValueError(f"Invalid edge format: {edge}")
                

            if (isinstance(source, type) and issubclass(source, Exception)): # Error edge

                self._error_edge_index[source].append(next)

            elif isinstance(source, tuple): # Error edge with nodes

                nodes = source[0] if isinstance(source[0], list) else [source[0]]
                et = source[1]

                for node in nodes:
                    self._error_edge_index[(node, et)].append(next)

            elif isinstance(source, list): # Multiple sources
                for s in source:
                    if properties.instant:
                        self._instant_edge_index[s].append(next)
                    else:
                        self._edge_index[s].append(next)

            elif isinstance(source, Node) or source is START: # Single source
                if properties.instant:
                    self._instant_edge_index[source].append(next)
                else:
                    self._edge_index[source].append(next)

            else:
                raise ValueError(f"Invalid edge source: {edge[0]}")



    async def __call__(self, state: T, shared: S) -> Tuple[T, S]:
        """
        Execute the graph based on the edges

        Args:
            state: State of the first generic type of the graph or a subtype
            shared: Shared of the second generic type of the graph or a subtype

        Returns:
            New State instance and the same Shared instance
        """

        try:

            # Hook
            for h in self.hooks: await h.on_graph_start(state, shared)

            
            next_nodes: list[Node[T, S]] = await self.get_next_nodes(state, shared, [START])


            while True:

                # Hook
                for h in self.hooks: await h.on_step_start(state, shared, next_nodes)

                if not next_nodes:
                    break # END

                # Run parallel
                result_states: list[T] = []

                try:

                    async with asyncio.TaskGroup() as tg:
                        for task in next_nodes:
                            
                            state_copy: T = state.model_copy(deep=True)
                            result_states.append(state_copy)

                            tg.create_task(self.task_wrapper(task, state_copy, shared))

                    state = await self.merge_states(state, result_states)


                except ExceptionGroup as eg:

                    # Hook
                    for h in self.hooks: await h.on_step_end(state, shared, next_nodes)
                    
                    next_nodes = await self.get_next_nodes_from_error(state, shared, eg)
                    
                else:
    
                    # Hook
                    for h in self.hooks: await h.on_step_end(state, shared, next_nodes)

                    next_nodes = await self.get_next_nodes(state, shared, next_nodes)


            # Hook
            for h in self.hooks: await h.on_graph_end(state, shared)


            return state, shared
        

        except Exception as e:
            
            # Hook
            for h in self.hooks:
                e = await h.on_error(e, state, shared)
                if e is None: 
                    break
            
            if e:
                raise e
        
            return state, shared


    async def task_wrapper(self, task: Node[T, S], state: T, shared: S):
        try:
            await task(state, shared)
        except Exception as err:
            err.source_node = task # type: ignore
            raise err
        

    
    async def get_next_nodes_from_error(self, state: T, shared: S, eg: ExceptionGroup) -> list[Node[T, S]]:

        next_nodes: list[Node[T, S]] = []
        unhandled: list[Exception] = []

        for e in eg.exceptions:

            source_node: Node[T, S] | None = getattr(e, "source_node", None)

            if not isinstance(source_node, Node):
                unhandled.append(e)
                continue

            if type(e) in self._error_edge_index:
                next_nodes.extend(
                    await self.resolve_next_types(state, shared, self._error_edge_index[type(e)])
                )

            if (source_node, type(e)) in self._error_edge_index:
                next_nodes.extend(
                    await self.resolve_next_types(state, shared, self._error_edge_index[(source_node, type(e))])
                )

        
        if unhandled:
            raise ExceptionGroup("Unhandled node exceptions", unhandled)

        return next_nodes



    async def get_next_nodes(self, state: T, shared: S, current_nodes: list[Node[T, S]] | list[Node[T, S] | Type[START]]) -> list[Node[T, S]]:
        """
        Args:
            state: The current state
            shared: The shared state
            current_nodes: The current nodes

        Returns:
           The list of the next nodes to run based on the current nodes and edges.
           If an edge is a callable, it will be called with the current state and shared state.
        """


        next_nodes: list[Node[T, S]] = []


        # Regular nodes
        for current_node in current_nodes:

            # Find the edge corresponding to the current node
            next_nodes.extend(
                await self.resolve_next_types(state, shared, 
                    self._edge_index[current_node]
                )
            )


        # Instant nodes
        current_instant_nodes: list[Node[T, S]] = next_nodes.copy()

        while True:

            current_next_types = [
                next
                for node in current_instant_nodes 
                for next in self._instant_edge_index[node]
            ]
            
            if not current_next_types:
                break
            
            current_instant_nodes = await self.resolve_next_types(state, shared, current_next_types)
            next_nodes.extend(current_instant_nodes)


        return next_nodes

    

    async def resolve_next_types(self, state: T, shared: S, next_types: list[NextType[T, S]]) -> list[Node[T, S]]:

        next_nodes: list[Node[T, S]] = []
        for next in next_types:

            if isinstance(next, type):
                assert next is END, "Only END is allowed as a type here"
                continue

            if isinstance(next, Node):
                next_nodes.append(next)
            
            else:
                res = next(state, shared)
                if inspect.isawaitable(res):
                    res = await res # for awaitables
                
                if isinstance(res, Node):
                    next_nodes.append(res)

        
        return next_nodes



    async def merge_states(self, current_state: T, result_states: list[T]) -> T:
        """
        Merges the result states into the current state.
        First the changes are calculated for each result state.
        Then the changes are checked for conflicts.
        If there are conflicts, a ChangeConflictException is raised.
        The changes are applied in the order of the result states list.

        Args:
            current_state: The current state
            result_states: The result states

        Returns:
            The new merged State instance.

        Raises:
            ChangeConflictException: If there are conflicts in the changes.
        """
            
        result_dicts = [state.model_dump() for state in result_states]
        current_dict = cast(dict[Hashable, Any], current_state.model_dump())
        state_class = type(current_state)

        changes_list: list[dict[tuple[Hashable, ...], Change]] = []

        for result_dict in result_dicts:

            changes_list.append(Diff.recursive_diff(current_dict, result_dict))
        

        # Hook
        for h in self.hooks: await h.on_merge_start(current_state, result_states, changes_list)


        conflicts = Diff.find_conflicts(changes_list)

        if conflicts:

            # Hook
            for h in self.hooks: await h.on_merge_conflict(current_state, result_states, changes_list, conflicts)

            raise ChangeConflictException(f"Conflicts detected after parallel execution: {conflicts}")


        for changes in changes_list:
            Diff.apply_changes(current_dict, changes)

        state: T = state_class.model_validate(current_dict)


        # Hook
        for h in self.hooks: await h.on_merge_end(current_state, result_states, changes_list, state)

        return state
