from abc import ABC, abstractmethod
from pydantic import BaseModel, ConfigDict
from typing import TypeVar, AsyncIterator
from types import TracebackType


T = TypeVar('T', covariant=True, default=object)

class Stream(ABC, AsyncIterator[T]):

    @abstractmethod
    async def aclose(self) -> None:
        pass

    @abstractmethod
    async def __anext__(self) -> T:
        pass

    async def __aenter__(self) -> "Stream[T]":
        return self

    async def __aexit__(
            self, exc_type: type[BaseException] | None, 
            exc: BaseException | None, 
            tb: TracebackType | None
        ) -> None: # Not handling exceptions here -> returns None

        await self.aclose()


class State(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=False) # for deep copy


class Shared(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)