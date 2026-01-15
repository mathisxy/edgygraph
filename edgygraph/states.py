from pydantic import BaseModel, Field
from typing import TypeVar, AsyncIterator, Protocol, Generic
from types import TracebackType


T = TypeVar('T', covariant=True, default=object)

class Stream(AsyncIterator[T], Protocol):
    async def aclose(self) -> None: ...

    async def __aenter__(self) -> "Stream[T]":
        return self

    async def __aexit__(
            self, exc_type: type[BaseException] | None, 
            exc: BaseException | None, 
            tb: TracebackType | None
        ) -> None: # Not handling exceptions here -> returns None

        await self.aclose()


class GraphState(BaseModel, Generic[T]):
    vars: dict[str, object] = Field(default_factory=dict)
    streams: dict[str, Stream[T]] = Field(default_factory=dict)