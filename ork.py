from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable, Literal, Optional, Union
from multiprocessing import Process,Manager
from multiprocessing import Queue as MPQueue
import logging
from queue import Empty
from enum import Enum 
logger = logging.getLogger(__name__)

# Constraints 
AnyTask : int  = -1 # A sentinel value 

@dataclass()
class OrkAtom:
    from_node : int # A edge starting from a node with this number
    to_node : int # A edge ending in this node 
    when : Optional[int] = None # If passed then when task with id N returns True 
    

@dataclass()
class Or:
    args : list[OrkAtom]

@dataclass()
class And:
    args : list[OrkAtom]

@dataclass()
class Neg:
    arg : OrkAtom

OrkConstraint = Union[OrkAtom | Or | And | Neg]
# Constraints End
# Utils Classes
@dataclass()
class FromEdge:
    from_node : int 
    cond : Optional[int] = None 
    

@dataclass()
class OrkTask:
    task_id : int
    task_func :Callable
    status : Literal['pending','running','completed','failed']
    depends_on : list[FromEdge]
    task_context_id : Optional[int] = None # Set when task is running
    task_process : Optional[Process] = None # Set when task is running

@dataclass()
class TaskContext:
    to_server : MPQueue
    from_server : MPQueue
# Util classes end

def get_opt(q:MPQueue):
    try:
        res = q.get_nowait()
        return res 
    except Empty:
        return None 


def start_server(init_task_context : TaskContext):
    wf = Workflow()
    wf._register_context(init_task_context)
    wf.start_server()


def worker_function(task_num : int , result_dict : dict,func,init_task_context):
    result = func(init_task_context)
    result_dict[task_num] = result 


class Workflow:
    """
    Not exposed
    """
    def __init__(self):
        # Task Node Objects
        self.manager = Manager()
        self.result_dict = self.manager.dict()
        self.task_graph : dict[int,OrkTask] = {}
        self.contexts : list[TaskContext ] = []
        self.next_node_number = 1 # 1 - indexed
        self._start = False

    @classmethod
    def create_server(cls) -> WorkflowClient:
        init_task_context = TaskContext(MPQueue(),MPQueue())
        server_process = Process(target=start_server,args=(init_task_context,))
        server_process.start()
        print("server pid",server_process.pid)
        return WorkflowClient(init_task_context)
    
    def _register_context(self,nq : TaskContext):
        self.contexts.append(nq)

    def _schedule_task(self,task_id : int):
        logger.info(f"Starting task {task_id}")
        task_obj = self.task_graph[task_id]
        #Setup q
        task_context = TaskContext(MPQueue(),MPQueue())
        self.contexts.append(task_context)
        task_obj.task_context_id = len(self.contexts) - 1
        #Setup process
        task_obj.task_process = Process(target=worker_function,args=(task_obj.task_id,self.result_dict,task_obj.task_func,task_context,))
        task_obj.task_process.start()

    def _all_deps_complete(self,task_id : int) -> bool:
        for from_edge in self.task_graph[task_id].depends_on:
            # By default enforce the dependency
            enforce_dep = True 
            # If there is a condition first check that the conditional task has completed
            if (cond_task_res_id := from_edge.cond) is not None:
                if self.task_graph[cond_task_res_id].status != 'completed':
                    return False 
                # If the result of the task is False then don't enforce the dependency
                cond_task_res = self.result_dict[cond_task_res_id]
                assert isinstance(cond_task_res,bool) , f"Expected {cond_task_res_id} to have a boolean result"
                # Only enforce if cond_task_res is True 
                enforce_dep =  cond_task_res 
           
            if not enforce_dep:
                continue

            # Now check if the dependent task has completed
            if self.task_graph[from_edge.from_node].status != 'completed':
                return False 
        return True 

                 
    def _any_deps_fail(self,task_id : int ) -> bool:
        for from_edge in self.task_graph[task_id].depends_on:
            # If there is a condition first check that the conditional task has completed
            if (cond_task_res_id := from_edge.cond) is not None and self.task_graph[cond_task_res_id].status == 'failed':
                    return True  
            if self.task_graph[from_edge.from_node].status == 'failed':
                return True  
        return False

    def _execute(self) -> bool:
        """
        Blocks until no pending tasks remaing in workflow
        Returns:
            If alive (tasks are still pending/running)
        """
        pending_task_ids = [k for k in self.task_graph if self.task_graph[k].status == 'pending']
        running_task_ids = [k for k in self.task_graph if self.task_graph[k].status == 'running']
        alive =  len(pending_task_ids) != 0  or len(running_task_ids) != 0 
        if not alive:
            return False
        # Update all running tasks that have completed
        for running_task_id in running_task_ids:
            task_obj = self.task_graph[running_task_id]
            proc_handle  = task_obj.task_process
            assert isinstance(proc_handle,Process) , f"Expected a process for running task {running_task_id}"
            if  proc_handle.is_alive():
                continue # If process is alive then continue
            new_status = 'completed' if proc_handle.exitcode == 0 else 'failed'

            proc_handle.join()
            proc_handle.close()
            self.task_graph
            task_obj.status = new_status
        # Schedule all pending tasks that can run
        for pending_task_id in pending_task_ids:
            pending_task_obj = self.task_graph[pending_task_id]
            all_complete = self._all_deps_complete(pending_task_id)
            failed_deps = self._any_deps_fail(pending_task_id)
            if all_complete:
                self._schedule_task(pending_task_id)
                pending_task_obj.status = 'running'
            #If dependent tasks failed then propagate failure
            elif not all_complete and failed_deps:
                logger.info(f"Marking {pending_task_id} as failed because {failed_deps} have failed")
                pending_task_obj.status = 'failed'
        pending_task_ids = [k for k in self.task_graph if self.task_graph[k].status == 'pending']
        running_task_ids = [k for k in self.task_graph if self.task_graph[k].status == 'running']
        alive =  len(pending_task_ids) != 0  or len(running_task_ids) != 0
        return alive 
       
        

    def _add_task(self,func: Callable,depends_on : Optional[list[int]] = None) -> int:
        if depends_on is None:
            depends_on = []
        # Check that all dependent tasks exists
        existing_ids = set(self.task_graph.keys())
        given_ids = set(depends_on)
        if len(missing_ids := (given_ids - existing_ids)) != 0:
            raise ValueError(f"Missing ids {missing_ids}")
            
        #Create task object
        from_edges = [FromEdge(dep_id,None) for dep_id in depends_on]
        task_obj = OrkTask(self.next_node_number,func,'pending',from_edges)
        self.task_graph[self.next_node_number] = task_obj
        self.next_node_number += 1 
        return task_obj.task_id
    

    

    
    # def _check_constraint(self,check_constraint : OrkConstraintApp):
    #     pass 
    
    # def _add_constraint(self,constraint: OrkConstraintApp):
    #     pass



    def _read_messages(self):
        for context in self.contexts:
            while (msg := get_opt(context.to_server)) is not None:
                msg_action,args = msg
                match msg_action:
                    case "add_task":
                        func,depends_on = args
                        res_task_id = self._add_task(func,depends_on)
                        #TODO: This could block but assume that the queue always has space
                        context.from_server.put(res_task_id)
                    case "start":
                        self._start = True 
    
    def start_server(self):
        """
        Blocks
        """
        alive = True
        print("Running server")
        while alive:
            self._read_messages()
            if self._start:
                alive = self._execute()
        return None



class WorkflowClient:
    def __init__(self, task_context:TaskContext):
        self.task_context = task_context


    def add_task(self,func: Callable,depends_on : Optional[list[int]] = None) -> int:
        self.task_context.to_server.put(("add_task",(func,depends_on)))
        res_task_id = self.task_context.from_server.get()
        print("got res task id",res_task_id)
        return res_task_id
    
    def start(self):
        self.task_context.to_server.put_nowait(("start",()))

    # def add_constraint(self,constraint : OrkConstraintApp):
    #     self.task_context.to_server.put_nowait(("add_constraint",(constraint,)))
