from pydantic import BaseModel
from typing import Any, Dict, Literal, Optional

# Seperate models for event types
# Why? 
# - Useful to disinguish events from internal data structures
# - Don't want internal data structures to depend on pydantic (expensive)
# - Give a clear model for viz frontend
class FromEdgeModel(BaseModel):
    from_node : int 
    cond : int  | None = None

class MessageEvent(BaseModel):
    event_type : Literal["message"] = "message"
    action : str 
    args : list 

class CreateNodeEvent(BaseModel):
    event_type : Literal["create_node"]     = "create_node"
    node_id : int 
    name : str 
    deps : list[FromEdgeModel]

class MarkStateTransition(BaseModel):
    event_type : Literal["mark_state_transition"] = "mark_state_transition"
    node_id : int
    new_state : Literal["conditioned","pending","running","completed","failed"]
    result : Optional[Any] = None
    error_msg : Optional[str] = None

class CreatePromiseEvent(BaseModel):
    event_type : Literal["create_promise"] = "create_promise"
    constraint_type : Literal["EdgePromise","NodePromise"]
    from_nodes : list[str | int ] | Literal["ALL"]
    to_nodes : list[str | int ] | Literal["ALL"]
    constraint_op : Optional[Literal["==","<=","<",">=",">"]] = None
    constraint_n : Optional[int] = None

class ConditionedTask(BaseModel):
    condition : Dict
    task_id : int 
    
class CreateConditionEvent(BaseModel):
    event_type : Literal["create_condition"] = "create_condition"
    case_groups : list[ConditionedTask]

#TODO: Add events for state transitions 

class Event(BaseModel):
    timestamp : float 
    event : MessageEvent | CreateNodeEvent | MarkStateTransition | CreatePromiseEvent | CreateConditionEvent


    