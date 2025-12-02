from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Literal, Optional
from multiprocessing import Process,Manager
from multiprocessing import Queue as MPQueue
import logging
from queue import Empty
from abc import ABC
import functools
logger = logging.getLogger(__name__)


# Constraints 
@dataclass()
class Quant(ABC):
    pass
@dataclass()
class NONE(Quant):
    pass

@dataclass()
class One(Quant):
    tasks : set[str]

@dataclass 
class Some(Quant):
    tasks : set[str]

@dataclass
class FS(Quant): 
    tasks : set[str] # Just a finite set


@dataclass
class All(Quant):
    pass 
    

class OrkPromise(ABC):
    applies : FS | All
    quant : Quant
    
# Constraints End
# Utils Classes
@dataclass(eq=True,frozen=True)
class FromEdge:
    from_node : int 
    cond : Optional[int] = None 
    
OrkTaskStatus  = Literal['creating','pending','running','completed','failed']
@dataclass()
class OrkTask:
    task_id : int
    task_func :Callable
    status : OrkTaskStatus
    depends_on : set[FromEdge]
    task_context_id : Optional[int] = None # Set when task is running
    task_process : Optional[Process] = None # Set when task is running

@dataclass()
class TaskContext:
    to_server : MPQueue
    from_server : MPQueue
    task_id : Optional[int] = None 
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

def worker_function(init_task_context : TaskContext, result_dict : dict,fn):
    result = fn(init_task_context)
    result_dict[init_task_context.task_id] = result 
    return result    
        

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
        self.wait_qs : list[MPQueue] = []

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
        task_context.task_id = task_obj.task_id
        task_obj.task_process = Process(target=worker_function,args=(task_context,self.result_dict,task_obj.task_func,))
        task_obj.task_process.start()

    def _all_deps_complete(self,task_id : int) -> bool:
        for dep_edge in self.task_graph[task_id].depends_on:
            #Check if there is a conditional and it has resolved
            if cond_task_id := dep_edge.cond:
                if not self.task_graph[cond_task_id].status == 'completed':
                    return False # Conditional has not resolved
                # If the conditional is false we don't need this edge
                result = self.result_dict[cond_task_id]
                assert isinstance(result,bool) , f"Expected result for {cond_task_id} to be a bool"
                if not result:
                    continue
            # Now check the edge itself
            # Compare the dependency's status instead of the task object itself
            if  self.task_graph[dep_edge.from_node].status != 'completed':
                return False
        return True 
                 
    def _any_deps_fail(self,task_id : int ) -> bool:
        for dep_edge in self.task_graph[task_id].depends_on:
            #Check if there is a conditional and it has failed 
            if cond_task_id := dep_edge.cond:
                if self.task_graph[cond_task_id].status == 'failed':
                    return True # Conditional failed
                # If the conditional is false we don't need this edge
                result = self.result_dict[cond_task_id]
                assert isinstance(result,bool) , f"Expected result for {cond_task_id} to be a bool"
                if not result: # If the conditional is false we don't care about the status of the dep
                    continue
            # Now check the edge itself
            # Compare the dependency's status instead of the task object itself
            if  self.task_graph[dep_edge.from_node].status == 'failed':
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
       
    def _add_task(self,func: Callable,depends_on : Optional[set[FromEdge]] = None ) -> int:
        if depends_on is None:
            depends_on = set()
        # Check that all dependent tasks exists
        existing_ids = set(self.task_graph.keys())
        given_ids = {dep.from_node for dep in depends_on}
        if len(missing_ids := (given_ids - existing_ids)) != 0:
            raise ValueError(f"Missing ids {missing_ids}")
            
        #Create task object
        task_obj = OrkTask(self.next_node_number,func,'creating',depends_on)
        self.task_graph[self.next_node_number] = task_obj
        self.next_node_number += 1 
        return task_obj.task_id


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
                    case 'commit_task':
                        for task_id in args:
                            assert self.task_graph[task_id].status == 'creating' , f"Expected status to be creating for {task_id}"
                            self.task_graph[task_id].status = 'pending'
                            context.from_server.put(None)
                    case "start":
                        self._start = True 
                    case "join":
                        # Block until all tasks complete
                        self.wait_qs.append(context.from_server)
                        
                        
    
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
        for wait_q in self.wait_qs:
            wait_q.put(None)
        self.wait_qs = []
        return None



class WorkflowClient:
    def __init__(self, task_context:TaskContext):
        self.task_context = task_context
        self.to_commit : list[int] = []

    
    def send_message(self,inp : tuple ):
        self.task_context.to_server.put(inp)
    def recv_message(self):
        return self.task_context.from_server.get()


    def add_task(self,func: Callable,depends_on : Optional[list[FromEdge]] = None) -> int:
        self.send_message(("add_task",(func,set(depends_on) if depends_on else None)))
        res_task_id = self.recv_message()
        self.to_commit.append(res_task_id)
        print("Created task id",res_task_id)
        return res_task_id
    
    def commit(self):
        self.send_message(('commit_task',tuple(self.to_commit)))
        _ = self.recv_message()
        print("Committed ",self.to_commit)
        self.to_commit = []
        return 
    def start(self):
        self.task_context.to_server.put_nowait(("start",()))

    def join(self):
        self.task_context.to_server.put_nowait(("join",()))
        _ = self.task_context.from_server.get()
        
