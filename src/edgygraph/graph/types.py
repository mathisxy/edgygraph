from __future__ import annotations
from typing import Callable, Awaitable, Any, TypeGuard, cast
from pydantic import BaseModel, ConfigDict, Field

from ..states import StateProtocol, SharedProtocol
from ..nodes import NavigationNode, Node, START, END, NodeConfig


type Flexible[V] = V | list[V]

# Branch types - Flattened for Typechecker Performance in BranchContainer

type BranchSource[T: StateProtocol, S: SharedProtocol] = (
      Node[T, S]
    | NavigationNode
    | type[START]
)

type BranchContent[T: StateProtocol, S: SharedProtocol] = (
      Node[T, S]
    | NodeWithConfig[T, S]
    | NavigationNode
    | ErrorSource[T, S]
    | type[START] 
    | None
    | Callable[[T, S], Flexible[Node[T, S] | NavigationNode | None]]
    | Callable[[T, S], Awaitable[Flexible[Node[T, S] | NavigationNode | None]]]
)

type BranchJoin[T: StateProtocol, S: SharedProtocol] = (
      Node[T, S]
    | NavigationNode
    | type[END]
    | None
)

type BranchContainer[T: StateProtocol, S: SharedProtocol] = tuple[BranchSource[T, S], *tuple[Flexible[BranchContent[T, S]], ...], BranchJoin[T, S]]

# Internal types

type Source[T: StateProtocol, S: SharedProtocol] = (
      Node[T, S]
    | NavigationNode
    | type[START]
)

type NodeWithConfig[T: StateProtocol, S: SharedProtocol] = tuple[NodeConfig, Node[T, S] | NavigationNode]

type ErrorSource[T: StateProtocol, S: SharedProtocol] = type[Exception] | tuple[type[Exception], Node[T, S]]

type UnresolvedSource[T: StateProtocol, S: SharedProtocol] = (
      Flexible[Source[T, S] | NodeWithConfig[T, S]]
    | ErrorSource[T, S]
)

type Next[T: StateProtocol, S: SharedProtocol] = (
      Node[T, S]
    | NavigationNode
    | None
)

type NextCallable[T: StateProtocol, S: SharedProtocol] = (
      Callable[[T, S], Flexible[Next[T, S]]]
    | Callable[[T, S], Awaitable[Flexible[Next[T, S]]]]
)

type UnresolvedNext[T: StateProtocol, S: SharedProtocol] = (
    Flexible[Next[T, S] | NodeWithConfig[T, S]] 
    | NextCallable[T, S]
)

#type NodeWithConfig[T: StateProtocol, S: SharedProtocol] = tuple[NodeConfig, Node[T, S]]

#type SingleSource[T: StateProtocol, S: SharedProtocol] = Node[T, S] | NavigationNode | type[START]
#type SingleSourceWithConfig[T: StateProtocol, S: SharedProtocol] = NodeWithConfig[T, S] | SingleSource[T, S]
#type Source[T: StateProtocol, S: SharedProtocol] = Flexible[SingleSource[T, S]]
#type SourceWithConfig[T: StateProtocol, S: SharedProtocol] = Flexible[SingleSourceWithConfig[T, S]]

#type SingleErrorSource[T: StateProtocol, S: SharedProtocol] = type[Exception] | tuple[type[Exception], Node[T, S]]
#type ErrorSource[T: StateProtocol, S: SharedProtocol] = Flexible[SingleErrorSource[T, S]]

#type SingleNext[T: StateProtocol, S: SharedProtocol] = Node[T, S] | NavigationNode | None
#type SingleNextWithConfig[T: StateProtocol, S: SharedProtocol] = NodeWithConfig[T, S] | SingleNext[T, S]
#type NextCallable[T: StateProtocol, S: SharedProtocol] = Callable[[T, S], ResolvedNext[T, S]] | Callable[[T, S], Awaitable[ResolvedNext[T, S]]]
#type ResolvedNext[T: StateProtocol, S: SharedProtocol] = Flexible[SingleNext[T, S]]
#type ResolvedNextWithConfig[T: StateProtocol, S: SharedProtocol] = Flexible[SingleNextWithConfig[T, S]]
#type Next[T: StateProtocol, S: SharedProtocol] = ResolvedNext[T, S] | NextCallable[T, S]
#type NextWithConfig[T: StateProtocol, S: SharedProtocol] = ResolvedNextWithConfig[T, S] | NextCallable[T, S] # Dynamic parameters cannot have a NodeConfig, because it is used for indexing.

#type BranchJoin[T: StateProtocol, S: SharedProtocol] = SingleNext[T, S] | type[END]


class Types[T: StateProtocol, S: SharedProtocol]:
    """
    Typeguards for runtime typechecking.
    """

    @classmethod
    def is_source(cls, x: Any) -> TypeGuard[Source[T, S]]:
        return (
            isinstance(x, Node) 
            or isinstance(x, NavigationNode) 
            or x is START
        )
    
    @classmethod
    def is_source_list(cls, x: Any) -> TypeGuard[list[Source[T, S]]]:
        return isinstance(x, list) and all(cls.is_source(n) for n in cast(list[Any], x))

    @classmethod
    def is_node_with_config(cls, x: Any) -> TypeGuard[NodeWithConfig[T, S]]:
        return (
            isinstance(x, tuple) and
            len(cast(tuple[Any], x)) == 2 and
            isinstance(x[0], NodeConfig) and
            isinstance(x[1], (Node, NavigationNode))
        )

    
    @classmethod
    def is_error_source(cls, x: Any) -> TypeGuard[ErrorSource[T, S]]:
        return (
            isinstance(x, type) and issubclass(x, Exception)
        ) or (
            isinstance(x, tuple) and len(cast(tuple[Any], x)) == 2 and isinstance(x[0], type) and issubclass(x[0], Exception) and isinstance(x[1], Node)
        )
    
    @classmethod
    def is_unresolved_source(cls, x: Any) -> TypeGuard[UnresolvedSource[T, S]]:
        return (
            cls.is_source(x)
            or cls.is_error_source(x) 
            or cls.is_node_with_config(x)
            or isinstance(x, list) and all(
                cls.is_source(n)
                or cls.is_node_with_config(n)
            for n in cast(list[Any], x)
            )
        )
    
    @classmethod
    def is_unresolved_source_only_flexible(cls, x: Any) -> TypeGuard[Flexible[Source[T, S] | NodeWithConfig[T, S]]]:
        return (
            cls.is_source(x)
            or cls.is_node_with_config(x)
            or isinstance(x, list) and all(
                cls.is_source(n)
                or cls.is_node_with_config(n)
            for n in cast(list[Any], x)
            )
        )
    
    @classmethod
    def is_next(cls, x: Any) -> TypeGuard[Next[T, S]]:
        return (
            isinstance(x, Node)
            or isinstance(x, NavigationNode)
            or x is None
        )
    
    @classmethod
    def is_next_list(cls, x: Any) -> TypeGuard[list[Next[T, S]]]:
        return isinstance(x, list) and all(cls.is_next(n) for n in cast(list[Any], x))
    
    @classmethod
    def is_next_callable(cls, x: Any) -> TypeGuard[NextCallable[T, S]]:
        """Limited to determine weither x is a callable and not another known class or type.
        Therefore it can be false positive for unknown classes or types, but it should not be false negative for callables.
        """
        return callable(x) and not (
            cls.is_source(x)
            or cls.is_next(x)
            or x is START
            or x is END
            or x is Exception
        )
    
    @classmethod
    def is_unresolved_next(cls, x: Any) -> TypeGuard[UnresolvedNext[T, S]]:
        return (
            cls.is_next(x) 
            or cls.is_node_with_config(x)
            or cls.is_next_callable(x) 
            or isinstance(x, list) and all(
                cls.is_next(n) 
                or cls.is_node_with_config(n) 
            for n in cast(list[Any], x)
            )
        )
    
    @classmethod
    def is_unresolved_next_only_flexible(cls, x: Any) -> TypeGuard[Flexible[Next[T, S] | NodeWithConfig[T, S]]]:
        return (
            cls.is_next(x) 
            or cls.is_node_with_config(x) 
            or isinstance(x, list) and all(
                cls.is_next(n) 
                or cls.is_node_with_config(n) 
            for n in cast(list[Any], x)
            )
        )
    

#     @classmethod
#     def is_node_with_config(cls, x: Any) -> TypeGuard[NodeWithConfig[T, S]]:
#         return (
#             isinstance(x, tuple) and
#             len(cast(tuple[Any], x)) == 2 and
#             isinstance(x[0], NodeConfig) and
#             isinstance(x[1], Node)
#         )


#     @classmethod
#     def is_single_next(cls, x: Any) -> TypeGuard[Next[T, S]]:
#         return (
#             x is None or
#             isinstance(x, Node)
#         )
    
#     @classmethod
#     def is_single_next_with_config(cls, x: Any) -> TypeGuard[SingleNextWithConfig[T, S]]:
#         return (
#             cls.is_single_next(x) or
#             cls.is_node_with_config(x)
#         )
    
#     @classmethod
#     def is_single_next_list(cls, x: Any) -> TypeGuard[list[Next[T, S]]]:
#         return isinstance(x, list) and all(cls.is_single_next(n) for n in cast(list[Any], x))
    
#     @classmethod
#     def is_single_next_with_config_list(cls, x: Any) -> TypeGuard[list[SingleNextWithConfig[T, S]]]:
#         return isinstance(x, list) and all(cls.is_single_next_with_config(n) for n in cast(list[Any], x))
    
#     @classmethod
#     def is_resolved_next(cls, x: Any) -> TypeGuard[ResolvedNext[T, S]]:
#         return (
#             cls.is_single_next(x) or
#             cls.is_single_next_list(x)
#         )
    
#     @classmethod
#     def is_resolved_next_with_config(cls, x: Any) -> TypeGuard[ResolvedNextWithConfig[T, S]]:
#         return (
#             cls.is_single_next_with_config(x) or
#             cls.is_single_next_with_config_list(x)
#         )
    
#     @classmethod
#     def is_next_callable(cls, x: Any) -> TypeGuard[NextCallable[T, S]]:
#         return callable(x) and not (
#             cls.is_any_source(x) or # includes Node, START, Exceptions
#             x is END
#         )
    

#     @classmethod
#     def is_next(cls, x: Any) -> TypeGuard[Next[T, S]]:
#         return (
#             cls.is_resolved_next(x) or
#             cls.is_next_callable(x)
#         )
    
#     @classmethod
#     def is_next_with_config(cls, x: Any) -> TypeGuard[NextWithConfig[T, S]]:
#         return (
#             cls.is_resolved_next_with_config(x) or
#             cls.is_next_callable(x)
#         )

#     @classmethod
#     def is_single_source(cls, x: Any) -> TypeGuard[SingleSource[T, S]]:
#         return (
#             isinstance(x, Node) or
#             x is START
#         )
    
#     @classmethod
#     def is_single_source_with_config(cls, x: Any) -> TypeGuard[SingleSourceWithConfig[T, S]]:
#         return cls.is_node_with_config(x) or cls.is_single_source(x)

#     @classmethod
#     def is_single_source_list(cls, x: Any) -> TypeGuard[list[SingleSource[T, S]]]:
#         return isinstance(x, list) and all(cls.is_single_source(n) for n in cast(list[Any], x))
    
#     @classmethod
#     def is_single_source_with_config_list(cls, x: Any) -> TypeGuard[list[SingleSourceWithConfig[T, S]]]:
#         return isinstance(x, list) and all(cls.is_single_source_with_config(n) for n in cast(list[Any], x))

#     @classmethod
#     def is_source(cls, x: Any) -> TypeGuard[BranchSource[T, S]]:
#         return (
#             cls.is_single_source(x) or
#             cls.is_single_source_list(x)
#         )
    
#     @classmethod
#     def is_source_with_config(cls, x: Any) -> TypeGuard[SourceWithConfig[T, S]]:
#         return (
#             cls.is_single_source_with_config(x) or
#             cls.is_single_source_with_config_list(x)
#         )

#     @classmethod
#     def is_single_error_source(cls, x: Any) -> TypeGuard[SingleErrorSource[T, S]]:
#         return (
#             (isinstance(x, type) and issubclass(x, Exception)) or
#             (isinstance(x, tuple) and len(cast(tuple[Any], x)) == 2 and isinstance(x[0], Node) and isinstance(x[1], type) and issubclass(x[1], Exception))
#         )
    
#     @classmethod
#     def is_single_error_source_list(cls, x: Any) -> TypeGuard[list[SingleErrorSource[T, S]]]:
#         return isinstance(x, list) and all(cls.is_single_error_source(n) for n in cast(list[Any], x))
    
#     @classmethod
#     def is_error_source(cls, x: Any) -> TypeGuard[ErrorSource[T, S]]:
#         return (
#             cls.is_single_error_source(x) or
#             cls.is_single_error_source_list(x)
#         )
    
#     @classmethod
#     def is_any_single_source(cls, x: Any) -> TypeGuard[SingleSourceWithConfig[T, S] | SingleErrorSource[T, S]]:
#         return cls.is_single_source_with_config(x) or cls.is_single_error_source(x)

#     @classmethod
#     def is_any_single_source_list(cls, x: Any) -> TypeGuard[list[SingleSourceWithConfig[T, S] | SingleErrorSource[T, S]]]:
#         return isinstance(x, list) and all(cls.is_any_single_source(n) for n in cast(list[Any], x))
    
#     @classmethod
#     def is_any_source(cls, x: Any) -> TypeGuard[SourceWithConfig[T, S] | ErrorSource[T, S]]:
#         return cls.is_source_with_config(x) or cls.is_error_source(x)

#     @classmethod
#     def is_branch_join(cls, x: Any) -> TypeGuard[BranchJoin[T, S]]:
#         return x is END or cls.is_next(x)


class ErrorConfig(BaseModel):
    """
    Configuration for the error edge.

    Attributes:
        propagate: If the error should be propagated to the next error edge. If False, the error is caught and the graph continues.
    """

    propagate: bool = False

class BaseEdge[T: StateProtocol, S: SharedProtocol](BaseModel):
    """
    Base class for edges.

    Attributes:
        next: The unresolved targets of the edge.
    """

    next: Flexible[Next[T, S]] | NextCallable[T, S]
    model_config = ConfigDict(arbitrary_types_allowed=True)

class Edge[T: StateProtocol, S: SharedProtocol](BaseEdge[T, S]):
    """
    An edge in a branch.

    Attributes:
        source: The source of the edge.
    """

    source: Source[T, S]

class ErrorEdge[T: StateProtocol, S: SharedProtocol](BaseEdge[T, S]):
    """
    An error edge in a branch.

    Attributes:
        source: The source of the error edge.
        config: The configuration of the error edge.
    """

    source: ErrorSource[T, S]
    config: ErrorConfig = Field(default_factory=ErrorConfig)

class BaseEntry[T: StateProtocol, S: SharedProtocol](BaseModel):
    """
    Base class for the values of edge indexing dictionaries of a branch.

    Do not instantiate directly.

    Attributes:
        next: The unresolved targets of the edge.
        index: The original index of the entry in the list of edges of the branch.
    """

    next: Flexible[Next[T, S]] | NextCallable[T, S]
    index: int
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def model_post_init(self, __context: Any):
        if type(self) is BaseEntry:
            raise Exception("BaseEntry is not meant to be instantiated directly.") # Safeguard


class Entry[T: StateProtocol, S: SharedProtocol](BaseEntry[T, S]):
    """
    A value of the edge indexing dictionary of a branch.

    Attributes:
        next: The unresolved targets of the edge.
        index: The original index of the entry in the list of edges.
    """

class ErrorEntry[T: StateProtocol, S: SharedProtocol](BaseEntry[T, S]):
    """
    A value of the error edge indexing dictionary of a branch.

    Attributes:
        next: The unresolved targets of the edge.
        index: The original index of the entry in the list of edges.
        propagate: If the error should be reraised. If False, the error is caught and the graph continues.
    """

    propagate: bool = Field(default=False)
    # config: ErrorConfig = Field(default_factory=ErrorConfig)


type Entries[T: StateProtocol, S: SharedProtocol] = Entry[T, S] | ErrorEntry[T, S]


class NextNode[T: StateProtocol, S: SharedProtocol](BaseModel):
    """
    A node that is the target of an edge.

    Attributes:
        node: The node.
        reached_by: The edge that targeted this node.
    """

    node: Node[T, S] | NavigationNode
    reached_by: Entries[T, S]
    model_config = ConfigDict(arbitrary_types_allowed=True)
