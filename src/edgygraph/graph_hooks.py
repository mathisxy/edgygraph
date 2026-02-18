from abc import ABC
from .nodes import Node
from .states import StateProtocol as State, SharedProtocol as Shared
from .diff import Change


class GraphHook[T: State, S: Shared](ABC):
    """
    Hooks for the graph execution.

    Hooks are called at different stages of the graph execution.
    They can be used to log, modify the state, or perform other actions.
    """
    async def on_graph_start(self, state: T, shared: S): pass
    async def on_step_start(self, state: T, shared: S, nodes: list[Node[T, S]]): pass
    async def on_step_end(self, state: T, shared: S): pass
    async def on_merge_start(self, state: T, result_states: list[T], changes: list[dict[str, "Change"]]): pass
    async def on_merge_conflict(self, state: T, result_states: list[T], changes: list[dict[str, Change]], conflicts: dict[str, list["Change"]]): pass
    async def on_merge_end(self, state: T, result_states: list[T], changes: list[dict[str, "Change"]], merged_state: T): pass
    async def on_graph_end(self, state: T, shared: S): pass