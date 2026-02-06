from .states import StateProtocol, SharedProtocol
from typing import Protocol, runtime_checkable
from abc import ABC, abstractmethod


class Node(ABC):
    """
    Represents a node in the graph.

    The generic types define the type of state and shared state that the node expects.
    Due to variance the node can also take any subtype of the state and shared state.

    The node must implement the `run` method, which will be called when the node is executed.
    The argument types of the `run` method define the type of state and shared state that the node expects.
    Due to variance the node can also take any subtype of the state and shared state.
    """
    
    @abstractmethod
    async def run(self, state: StateProtocol, shared: SharedProtocol) -> None:
        """
        Runs the node with the given state and shared state from the graph.

        Operations on the state are merged in the graphs state after the node has finished.
        Therefore, the state is not shared between nodes and operations are safe.

        Operations on the shared state are reflected in all parallel running nodes.
        Therefore, the shared state is shared between nodes and operations are not safe without using the Lock.
        The lock can be accessed via `shared.lock`.

        Args:
            state: The state of the graph.
            shared: The shared state of the graph.

        Returns:
            None. The instance references of the arguments are used in the graph to enable variance.
        """
        pass



@runtime_checkable
class NodeProtocol[T: StateProtocol, S: SharedProtocol](Protocol):
    """
    Protocol for nodes in the graph. This is used for type checking and to allow for more flexible node definitions.
    """
    
    async def run(self, state: T, shared: S) -> None:
        """
        Args:
            state: The state of the graph.
            shared: The shared state of the graph.

        Returns:
            None. The instance references of the arguments are used in the graph to enable variance.
        """
        pass


class START:
    """
    Represents a start node
    """
    pass

class END:
    """
    Represents an end node
    """
    pass