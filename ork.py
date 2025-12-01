from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Literal, Optional

from multiprocessing import Process
import logging
logger = logging.getLogger(__name__)
def register(func):
    def wrapper(*args, **kwargs):
        print("Running ", func.__name__)
        result = func(*args, **kwargs)
        print("Finished running ", func.__name__)
        return result
    return wrapper

@dataclass()
class OrkTask:
    task_id : int
    func : Callable
    status : Literal['pending','running','completed','failed']
    depends_on:  list[int]

class Workflow:
    def __init__(self):
        # Node -> list of dependent nodes
        self.task_graph : dict[int,OrkTask] = {}
        self.task_handles : dict[int,Process] = {}
        self.next_node_number = 0 # 0 - indexed


    def add_task(self,func: Callable,depends_on : Optional[list[int]] = None) -> int:
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
    
    def _schedule_task(self,task_id : int):
        logger.info(f"Starting task {task_id}")
        task_obj = self.task_graph[task_id]
        self.task_handles[task_obj.task_id] = Process(target=task_obj.func)
        proc_handle = self.task_handles[task_obj.task_id]
        proc_handle.start()
        
    
    def execute(self):
        """
        Blocks until no pending tasks remaing in workflow
        """
        pending_task_ids = [k for k in self.task_graph if self.task_graph[k].status == 'pending']
        running_task_ids = [k for k in self.task_graph if self.task_graph[k].status == 'running']
        while pending_task_ids or running_task_ids:
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
        logger.info("Finished task execution for all tasks")
        return



        

