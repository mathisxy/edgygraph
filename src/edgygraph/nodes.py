from __future__ import annotations
from abc import ABC, abstractmethod
from copy import copy
from pydantic import BaseModel
from typing import Literal, Self
from importlib import metadata

from .states import StateProtocol, SharedProtocol

class Node[T: StateProtocol = StateProtocol, S: SharedProtocol = SharedProtocol](ABC):
    """
    Represents a node in the graph.

    The generic types define the type of state and shared state that the node expects.
    Due to variance or the usage of protocols, the node can also take any subtype of the state and shared state.

    The node must implement the `__call__` method to run the node.
    """

    dependencies: set[str] = set()
    """
    The pip dependencies of the node (python packages). On initialization of the node a check is performed if the dependencies are installed with importlib. If not an error is raised.
    
    The dependencies are collected from all parent classes. That means if a parent class has dependencies, the child class will also have those, but the child class can add more by setting the `dependencies` attribute without the need to repeat the parent classes dependencies.
    """

    @classmethod
    def check_dependencies(cls) -> None:
        """
        Checks if the dependencies of the node are installed.

        Raises:
            ImportError: If a dependency is not installed.
        """
        for dependency in cls.dependencies:
            try:
                metadata.version(dependency)
            except metadata.PackageNotFoundError:
                raise ImportError(f"Dependency {dependency} not found but required by node {cls.__name__}. Please install it with pip install {dependency}")

    def __init_subclass__(cls) -> None:
        """
        Called when a subclass is created. This is used to collect the dependencies of the node from parent classes.

        Works recursively because it is called on each inheritance level. Therefore the dependencies of all parent classes are collected and stored in the class variable `dependencies` of the child class.
        """
        super().__init_subclass__()

        cls.dependencies = cls.dependencies.copy() # To not modify the class variable of the parent

        for base in cls.__bases__: # Called on each inheritance level, therefore its recursive
            if not issubclass(base, Node):
                continue
            cls.dependencies.update(base.dependencies)

    def __init__(self) -> None:
        self.check_dependencies()

    
    @abstractmethod
    async def __call__(self, state: T, shared: S) -> None:
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

    
    def copy(self) -> Self:
        """
        Creates a copy of the node. This can be used when the same node is used multiple times in the graph.

        Returns:
            A copy of the node.
        """
        return copy(self)
    

    def __neg__(self) -> tuple[NodeConfig, Self]:
        """
        Negates the node. This can be used to indicate that the node should only be used as a next and not as a source.
        """
        return (NodeConfig(operator="neg"), self)

    def __pos__(self) -> tuple[NodeConfig, Self]:
        """
        Positivizes the node. This can be used to indicate that the node should only be used as a source and not as a next.
        """
        return (NodeConfig(operator="pos"), self)


class NavigationNode():
    """
    Represents a navigation node in the graph. This is a node that does not perform any operations on the state or shared state, but is used for navigation in the graph.

    This can be used to create branches in the graph or to indicate the start or end of the graph.
    """

    def __neg__(self) -> tuple[NodeConfig, Self]:
        """
        Negates the node. This can be used to indicate that the node should only be used as a next and not as a source.
        """
        return (NodeConfig(operator="neg"), self)

    def __pos__(self) -> tuple[NodeConfig, Self]:
        """
        Positivizes the node. This can be used to indicate that the node should only be used as a source and not as a next.
        """
        return (NodeConfig(operator="pos"), self)


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


class NodeConfig(BaseModel, frozen=True):

    operator: Literal["pos", "neg", None] = None

    @property
    def only_next(self) -> bool:
        return self.operator == "neg"
    
    @property
    def only_source(self) -> bool:
        return self.operator == "pos"