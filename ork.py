from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Literal, Optional
from multiprocessing import Process
from multiprocessing import Queue as MPQueue
import logging
from queue import Empty
logger = logging.getLogger(__name__)
# _TASK_REGISTRY : dict[str,Callable] = {}
# def register(name : str):
#     if name in _TASK_REGISTRY:
#         raise ValueError(f"{name} is already registred for function {_TASK_REGISTRY[name].__name__}")
#     def wrapper(func):
#         func._ork_registred_name = name
#         _TASK_REGISTRY[name] = func 
#         def fn_args(*args,**kwargs):
#             print("Running ", func.__name__)
#             result = func(*args, **kwargs)
#             print("Finished running ", func.__name__)
#             return result
#         return fn_args
#     return wrapper
def get_opt(q:MPQueue):
    try:
        res = q.get_nowait()
        return res 
    except Empty:
        return None 


@dataclass()
class OrkTask:
    task_id : int
    task_func :Callable
    status : Literal['pending','running','completed','failed']
    depends_on:  list[int]
    task_context_id : Optional[int] = None # Set when task is running

@dataclass()
class TaskContext:
    to_server : MPQueue
    from_server : MPQueue

def start_server(init_task_context : TaskContext):
    wf = Workflow()
    wf._register_context(init_task_context)
    wf.start_server()

class Workflow:
    """
    Not exposed
    """
    def __init__(self):
        # Node -> list of dependent nodes
        self.task_graph : dict[int,OrkTask] = {}
        self.task_handles : dict[int,Process] = {}
        self.next_node_number = 1 # 1 - indexed
        self.contexts : list[TaskContext ] = []
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
        self.task_handles[task_obj.task_id] = Process(target=task_obj.task_func,args=(task_context,))
        proc_handle = self.task_handles[task_obj.task_id]
        proc_handle.start()
       
        

    def _add_task(self,func: Callable,depends_on : Optional[list[int]] = None) -> int:
        if depends_on is None:
            depends_on = []
        # Check that all dependent tasks exists
        existing_ids = set(self.task_graph.keys())
        given_ids = set(depends_on)
        if len(missing_ids := (given_ids - existing_ids)) != 0:
            raise ValueError(f"Missing ids {missing_ids}")
            
        #Create task object
        task_obj = OrkTask(self.next_node_number,func,'pending',list(depends_on))

        self.task_graph[self.next_node_number] = task_obj
        self.next_node_number += 1 
        return task_obj.task_id
    

    
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
            proc_handle  = self.task_handles[running_task_id]
            if  proc_handle.is_alive():
                continue # If process is alive then continue
            new_status = 'completed' if proc_handle.exitcode == 0 else 'failed'
            proc_handle.close()
            del self.task_handles[running_task_id]
            self.task_graph[running_task_id].status = new_status
        # Schedule all pending tasks that can run
        for pending_task_id in pending_task_ids:
            pending_task_obj = self.task_graph[pending_task_id]
            all_complete = all([ self.task_graph[dep_id].status == 'completed'  for dep_id in pending_task_obj.depends_on ])
            failed_deps = [ dep_id   for dep_id in pending_task_obj.depends_on if self.task_graph[dep_id].status == 'failed']
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