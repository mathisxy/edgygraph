from abc import ABC, abstractmethod
from .states import State, Shared
from typing import TypeVar, Generic


T = TypeVar('T', bound=State, contravariant=True)
S = TypeVar('S', bound=Shared, contravariant=True)

class Node(ABC, Generic[T, S]):
    
    @abstractmethod
    async def run(self, state: T, shared: S) -> None:
        pass