from pydantic import BaseModel
from typing import Literal

class MessageEvent(BaseModel):
    action : str 
    args : list 

class FromEdge(BaseModel):
    src_task_id : str 
    cond_task_id : str | None = None

class CreateNodeEvent(BaseModel):
    node_id : str 
    deps : list[FromEdge]

class CreatePromiseEvent(BaseModel):
    edge_promise : str
    expected_count : int

class Event(BaseModel):
    timestamp : float 
    event : MessageEvent


    