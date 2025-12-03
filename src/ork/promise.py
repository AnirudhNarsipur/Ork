from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Literal
from abc import ABC
ListTaskOrTaskType = list[Callable | int]
ListTaskType = list[Callable]
class All:
      __slots__ = ()
      def __repr__(self): return "ALL"

class Promise(ABC):
    pass
COMPOPS = Literal["==","<=","<",">=",">"]
class ConstraintOpsMixin:
      def _constraint(self, op: COMPOPS, value: int) -> ConstrainedPromise:
          if not isinstance(value, int):
              raise TypeError("Only integers are allowed for constraints")
          return ConstrainedPromise(self, op, value) # type: ignore[arg-type]

      def __eq__(self, value: object) -> ConstrainedPromise:  # type: ignore[override]
          return self._constraint("==", value)  # type: ignore[arg-type]

      def __le__(self, value: int) -> ConstrainedPromise:
          return self._constraint("<=", value)

      def __lt__(self, value: int) -> ConstrainedPromise:
          return self._constraint("<", value)

      def __ge__(self, value: int) -> ConstrainedPromise:
          return self._constraint(">=", value)

      def __gt__(self, value: int) -> ConstrainedPromise:
          return self._constraint(">", value)
class EdgePromise(ConstraintOpsMixin,Promise):
    def __init__(self, from_nodes: ListTaskOrTaskType | All, to_nodes: ListTaskOrTaskType | All):
        self.from_nodes = from_nodes
        self.to_nodes = to_nodes

class NodePromise(ConstraintOpsMixin,Promise):
    # To node must be list task type as we don't know any ids yet
    def __init__(self,from_nodes : ListTaskOrTaskType | All,to_nodes : ListTaskType | All) -> None:
        super().__init__()
        self.from_nodes = from_nodes
        self.to_nodes = to_nodes

@dataclass(eq=True, frozen=True)
class ConstrainedPromise:
    promise : EdgePromise | NodePromise
    op : Literal["==","<=","<",">=",">"]
    n : int 
