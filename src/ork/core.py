from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass
import json
import time
from typing import Callable, Literal, Optional, Sequence
from multiprocessing import Process, Manager
from multiprocessing import Queue as MPQueue
import logging
from queue import Empty
from abc import ABC
from enum import Enum
from datetime import datetime
import uuid
from pydantic import BaseModel

from ork.event import Event, MessageEvent
from .promise import EdgePromise,NodePromise,ConstrainedPromise,All,ListTaskOrTaskType,ListTaskType,COMPOPS
from pathlib import Path
logger = logging.getLogger(__name__)

# Constraints End
# Utils Classes
@dataclass(eq=True, frozen=True)
class FromEdge:
    from_node: int
    cond: Optional[int] = None


OrkTaskStatus = Literal["creating", "pending", "running", "completed", "failed"]


@dataclass()
class OrkTask:
    task_id: int
    task_func: Callable
    status: OrkTaskStatus
    depends_on: set[FromEdge]
    parent_id: Optional[int] = None  # None only for the root task
    task_context_id: Optional[int] = None  # Set when task is running
    task_process: Optional[Process] = None  # Set when task is running


@dataclass()
class ClientContext:
    to_server: MPQueue
    from_server: MPQueue


@dataclass()
class TaskContext(ClientContext):
    task_id: int


@dataclass(eq=True, frozen=True)
class TypedTask:
    name: str  # The __qualname__ of the function
    task_id: int


# Util classes end


def get_opt(q: MPQueue):
    try:
        res = q.get_nowait()
        return res
    except Empty:
        return None


def start_server(init_task_context: TaskContext):
    wf = Workflow()
    wf._register_context(init_task_context)
    wf.start_server()


def worker_function(init_task_context: TaskContext, result_dict: dict, fn):
    result = fn(init_task_context)
    result_dict[init_task_context.task_id] = result
    return result


def applicable(rule_applies: ListTaskOrTaskType | All, typed_task: TypedTask) -> bool:
    match rule_applies:
        case All():
            return True  # All rules apply always
        case list(rule_task_list):
            for rule_task in rule_task_list:
                if isinstance(rule_task, int) and typed_task.task_id == rule_task:
                    return True
                elif (
                    isinstance(rule_task, Callable)
                    and typed_task.name == rule_task.__qualname__
                ):
                    return True
    return False


class ConstraintChecker:
    def __init__(self) -> None:
        # Forward graph
        self.id_to_name : dict[int, str] = {}
        self.current_edges: dict[int, set[int]] = defaultdict(set)  # Map from resource to list of task ids using it

        self.spawn_map : dict[int,set[int]] = defaultdict(set) # Map from spawner task id to spawned task ids
        # Store all constraints together
        self.constraints: list[ConstrainedPromise] = []

    def _check_op(self, lhs: int, op: COMPOPS, rhs: int) -> bool:
        match op:
            case "==":
                return lhs == rhs
            case "<=":
                return lhs <= rhs
            case "<":
                return lhs < rhs
            case ">=":
                return lhs >= rhs
            case ">":
                return lhs > rhs
        raise ValueError(f"Unknown op {op}")
    
    def get_ids_from_tasklist(self,task_list : ListTaskOrTaskType | All) -> list[int]:
        match task_list:
            case All():
                # TODO: just maintain a list of all task ids to avoid this
                st : set[int] = set()
                for k,v in self.spawn_map.items():
                    st.add(k)
                    st.update(v)
                return list(st)

                    
            case list(tl):
                res = []
                for task in tl:
                    if isinstance(task, int):
                        res.append(task)
                    elif isinstance(task, Callable):
                        for task_id, task_name in self.id_to_name.items():
                            if task_name == task.__qualname__:
                                res.append(task_id)
                return res
            case _:
                raise ValueError("Invalid task list")
       

    def _check_edge_constraint(self,edge_promise : EdgePromise,op : COMPOPS,n : int) -> bool:
        # For each node in from count the number of outgoing edges to to_nodes
        from_node_ids = self.get_ids_from_tasklist(edge_promise.from_nodes)
        to_node_ids = self.get_ids_from_tasklist(edge_promise.to_nodes)
        count = 0 
        to_node_id_set = set(to_node_ids)
        for from_id in from_node_ids:
            count += len(self.current_edges[from_id].intersection(to_node_id_set))
        return self._check_op(count,op,n)


    
    
    def _check_node_constraint(self,node_promise : NodePromise,op : COMPOPS,n : int) -> bool:
        # For each node in from count the number of spawned nodes in to_nodes
    
        from_node_ids = self.get_ids_from_tasklist(node_promise.from_nodes)
        to_node_ids = self.get_ids_from_tasklist(node_promise.to_nodes) # type: ignore[arg-type]
        count = 0 
        to_node_id_set = set(to_node_ids)
        for from_id in from_node_ids:
            count += len(self.spawn_map[from_id].intersection(to_node_id_set))
        return self._check_op(count,op,n)
        

    def _check_constraint(
        self, constraint_indexes : Sequence[int]) -> bool:
        for idx in constraint_indexes:
                constraint = self.constraints[idx]
                match  constraint.promise:
                    case EdgePromise():
                        if not self._check_edge_constraint(constraint.promise, constraint.op, constraint.n):
                            return False
                    case NodePromise():
                        if not self._check_node_constraint(constraint.promise, constraint.op, constraint.n):
                            return False
        return True
    
    def _task_relevant_constraints(self,task_id :int) -> list[int]:
        res = []
        for idx in range(len(self.constraints)):
            constraint = self.constraints[idx]
            typed_task = TypedTask(self.id_to_name[task_id],task_id)
            match constraint.promise:
                case EdgePromise():
                    promise : EdgePromise = constraint.promise # type: ignore[assignment]
                    outgoing = applicable(promise.from_nodes,typed_task)
                    incoming = applicable(promise.to_nodes,typed_task) # type: ignore[assignment]
                    if outgoing or incoming:
                        res.append(idx) 
                case NodePromise():
                    promise : NodePromise = constraint.promise # type: ignore[assignment]
                    outgoing = applicable(promise.from_nodes,typed_task)
                    incoming = applicable(promise.to_nodes,typed_task) # type: ignore[assignment]
                    if outgoing or incoming:
                        res.append(idx)
        return res 

    def add_deps(
        self,spawn_task : TypedTask, n1: TypedTask, n2: list[TypedTask]
    ) -> bool:  # False if dependency causes a violation
        applicable_constraints = []
        self.spawn_map[spawn_task.task_id].add(n1.task_id)
        self.id_to_name[n1.task_id] = n1.name
        applicable_constraints.extend(self._task_relevant_constraints(n1.task_id))
        applicable_constraints.extend(self._task_relevant_constraints(spawn_task.task_id))
        for n2_task in n2:
            self.id_to_name[n2_task.task_id] = n2_task.name
            self.current_edges[n2_task.task_id].add(n1.task_id)
            applicable_constraints.extend(self._task_relevant_constraints(n2_task.task_id))
        # Check all applicable constraints
        return self._check_constraint(applicable_constraints)

    def add_constraint(self, constraint: ConstrainedPromise) -> bool:
        self.constraints.append(constraint)
        return self._check_constraint([len(self.constraints) - 1])
        


class ExecStatus(Enum):
    CONTINUE = 1
    SHUTDOWN = 2
    CLEAN_EXIT = 3

def serialize_arg(arg):
    if isinstance(arg, Callable):
        return arg.__qualname__
    elif isinstance(arg, FromEdge):
        return {"from_node": arg.from_node, "cond": arg.cond}
    elif isinstance(arg, ConstrainedPromise):
        return {
            "promise": serialize_arg(arg.promise),
            "op": arg.op,
            "n": arg.n,
        }
    elif isinstance(arg, EdgePromise) or isinstance(arg, NodePromise):
        return {
            "from_nodes": [i if isinstance(i, int) else i.__qualname__ for i in (arg.from_nodes if isinstance(arg.from_nodes, list) else [])] if not isinstance(arg.from_nodes, All) else "ALL",
            "to_nodes": [i if isinstance(i, int) else i.__qualname__ for i in (arg.to_nodes if isinstance(arg.to_nodes, list) else [])] if not isinstance(arg.to_nodes, All) else "ALL",
        }
    else:
        return str(arg)

class Workflow:
    """
    Not exposed
    """

    def __init__(self):
        # Task Node Objects
        self.workflow_id : str = uuid.uuid7().hex
        self.manager = Manager()
        self.result_dict = self.manager.dict()
        self.task_graph: dict[int, OrkTask] = {}
        self.contexts: list[ClientContext] = []
        self.next_node_number = 1  # 1 - indexed
        self._start = False
        self.wait_qs: list[MPQueue] = []
        self.constraint_checker = ConstraintChecker()
        event_log_path = Path("event_logs") / f"workflow_{self.workflow_id}_events.jsonl"
        event_log_path.parent.mkdir(parents=True, exist_ok=True)
        self.event_log_file_handle = open(event_log_path,"a")

    @classmethod
    def create_server(cls) -> WorkflowClient:
        init_task_context = ClientContext(MPQueue(), MPQueue())
        server_process = Process(target=start_server, args=(init_task_context,))
        server_process.start()
        print("server pid", server_process.pid)
        return WorkflowClient(init_task_context)

    def _register_context(self, nq: TaskContext):
        self.contexts.append(nq)

    def _schedule_task(self, task_id: int):
        logger.info(f"Starting task {task_id}")
        task_obj = self.task_graph[task_id]
        # Setup q
        task_context = TaskContext(MPQueue(), MPQueue(), task_id)
        self.contexts.append(task_context)
        task_obj.task_context_id = len(self.contexts) - 1
        # Setup process
        task_obj.task_process = Process(
            target=worker_function,
            args=(
                task_context,
                self.result_dict,
                task_obj.task_func,
            ),
        )
        task_obj.task_process.start()

    def _all_deps_complete(self, task_id: int) -> bool:
        for dep_edge in self.task_graph[task_id].depends_on:
            # Check if there is a conditional and it has resolved
            if cond_task_id := dep_edge.cond:
                if not self.task_graph[cond_task_id].status == "completed":
                    return False  # Conditional has not resolved
                # If the conditional is false we don't need this edge
                result = self.result_dict[cond_task_id]
                assert isinstance(result, bool), (
                    f"Expected result for {cond_task_id} to be a bool"
                )
                if not result:
                    continue
            # Now check the edge itself
            # Compare the dependency's status instead of the task object itself
            if self.task_graph[dep_edge.from_node].status != "completed":
                return False
        return True

    def _any_deps_fail(self, task_id: int) -> bool:
        for dep_edge in self.task_graph[task_id].depends_on:
            # Check if there is a conditional and it has failed
            if cond_task_id := dep_edge.cond:
                if self.task_graph[cond_task_id].status == "failed":
                    return True  # Conditional failed
                # If the conditional is false we don't need this edge
                if self.task_graph[cond_task_id].status != "completed":
                    continue
                result = self.result_dict[cond_task_id]
                assert isinstance(result, bool), (
                    f"Expected result for {cond_task_id} to be a bool"
                )
                if not result:  # If the conditional is false we don't care about the status of the dep
                    continue
            # Now check the edge itself
            # Compare the dependency's status instead of the task object itself
            if self.task_graph[dep_edge.from_node].status == "failed":
                return True
        return False

    def _execute(self) -> ExecStatus:
        """
        Blocks until no pending tasks remaing in workflow
        Returns:
            If alive (tasks are still pending/running)
        """
        pending_task_ids = [
            k for k in self.task_graph if self.task_graph[k].status == "pending"
        ]
        running_task_ids = [
            k for k in self.task_graph if self.task_graph[k].status == "running"
        ]
        alive = len(pending_task_ids) != 0 or len(running_task_ids) != 0
        if not alive:
            return ExecStatus.CLEAN_EXIT
        # Update all running tasks that have completed
        for running_task_id in running_task_ids:
            task_obj = self.task_graph[running_task_id]
            proc_handle = task_obj.task_process
            assert isinstance(proc_handle, Process), (
                f"Expected a process for running task {running_task_id}"
            )
            if proc_handle.is_alive():
                continue  # If process is alive then continue
            new_status = "completed" if proc_handle.exitcode == 0 else "failed"

            proc_handle.join()
            proc_handle.close()
            self.task_graph
            task_obj.status = new_status
            task_obj.task_process = None  # Unset the process handle
        # Schedule all pending tasks that can run
        for pending_task_id in pending_task_ids:
            pending_task_obj = self.task_graph[pending_task_id]
            all_complete = self._all_deps_complete(pending_task_id)
            failed_deps = self._any_deps_fail(pending_task_id)
            if all_complete:
                self._schedule_task(pending_task_id)
                pending_task_obj.status = "running"
            # If dependent tasks failed then propagate failure
            elif not all_complete and failed_deps:
                logger.info(
                    f"Marking {pending_task_id} as failed because {failed_deps} have failed"
                )
                pending_task_obj.status = "failed"
        pending_task_ids = [
            k for k in self.task_graph if self.task_graph[k].status == "pending"
        ]
        running_task_ids = [
            k for k in self.task_graph if self.task_graph[k].status == "running"
        ]
        alive = len(pending_task_ids) != 0 or len(running_task_ids) != 0
        return ExecStatus.CONTINUE if alive else ExecStatus.CLEAN_EXIT

    def _shutdown(self, clean: bool = False):
        self._start = False
        # Clean up all processes
        for task_id in self.task_graph:
            task_obj = self.task_graph[task_id]
            if task_obj.task_process is not None:
                task_obj.task_process.terminate()
                task_obj.task_process.join()
                task_obj.task_process.close()

        # Unblock all waiting clients
        for wait_q in self.wait_qs:
            wait_q.put(clean)  # Bad Exit
        # Close all client connections
        for context in self.contexts:
            context.to_server.close()
            context.from_server.close()
        # Close event log file
        self.event_log_file_handle.close()

    def _add_task(
        self,
        func: Callable,
        depends_on: Optional[set[FromEdge]] = None,
        spawn_id: Optional[int] = None,
    ) -> int:
        if depends_on is None:
            depends_on = set()
        # Check that all dependent tasks exists
        existing_ids = set(self.task_graph.keys())
        given_ids = {dep.from_node for dep in depends_on}
        if len(missing_ids := (given_ids - existing_ids)) != 0:
            raise ValueError(f"Missing ids {missing_ids}")

        # Create task object
        typed_deps = [
            TypedTask(
                self.task_graph[dep.from_node].task_func.__qualname__, dep.from_node
            )
            for dep in depends_on
        ]
        if not self.constraint_checker.add_deps(TypedTask(self.task_graph[spawn_id].task_func.__qualname__, spawn_id) if spawn_id is not None else TypedTask("_ROOT",0),
            TypedTask(func.__qualname__, self.next_node_number), typed_deps
        ):
            return -1  # Dependency addition failed due to constraint violation

        task_obj = OrkTask(
            self.next_node_number, func, "creating", depends_on, parent_id=spawn_id
        )
        self.task_graph[self.next_node_number] = task_obj
        self.next_node_number += 1
        return task_obj.task_id

    def _add_promise(self, promise: ConstrainedPromise):
        return self.constraint_checker.add_constraint(promise)

    def _read_messages(self) -> bool:
        for context in self.contexts:
            while (msg := get_opt(context.to_server)) is not None:

                msg_action, args = msg
                message_event = Event(timestamp=datetime.now().timestamp(), event=MessageEvent(action=msg_action, args=[serialize_arg(a) for a in args]))
                self.event_log_file_handle.write(message_event.model_dump_json() + "\n")
                self.event_log_file_handle.flush() # Flush to display immediately
                match msg_action:
                    case "add_task":
                        func, depends_on, spawn_id = args
                        res_task_id = self._add_task(func, depends_on, spawn_id)
                        if res_task_id == -1:
                            return False
                        # TODO: This could block but assume that the queue always has space
                        context.from_server.put(res_task_id)
                    case "commit_task":
                        for task_id in args:
                            assert self.task_graph[task_id].status == "creating", (
                                f"Expected status to be creating for {task_id}"
                            )
                            self.task_graph[task_id].status = "pending"
                            context.from_server.put(None)
                    case "start":
                        self._start = True
                    case "wait":
                        # Block until all tasks complete
                        self.wait_qs.append(context.from_server)
                    case "add_promise":
                        (promise,) = args
                        if not self._add_promise(promise):
                            print(
                                "Constraint violation detected when adding promise, shutting down"
                            )
                            return False
                        context.from_server.put(None)

        return True

    def start_server(self):
        """
        Blocks
        """
        print("Running server")
        exec_status = ExecStatus.CLEAN_EXIT
        while True:
            if not self._read_messages():
                break
            if not self._start:
                continue
            match exec_status := self._execute():
                case ExecStatus.CONTINUE:
                    continue
                case ExecStatus.CLEAN_EXIT:
                    break
                case ExecStatus.SHUTDOWN:
                    print(
                        "Constraint violation detected during execution, shutting down"
                    )
                    break

        self._shutdown(exec_status == ExecStatus.CLEAN_EXIT)
        return None


class WorkflowClient:
    def __init__(self, task_context: ClientContext):
        self.task_context = task_context
        self.to_commit: list[int] = []

    def send_message(self, inp: tuple):
        self.task_context.to_server.put(inp)

    def recv_message(self):
        return self.task_context.from_server.get()

    def add_task(
        self, func: Callable, depends_on: Optional[list[FromEdge]] = None
    ) -> int:
        """
        Args:
            func: The function to run as a task
            depends_on: A list of FromEdge objects representing dependencies. If a dependency has a conditional task associated with it, the edge will only be considered if the conditional task returns True.
        A task is "created" but not schedelued until commit() is called by the client.
        Returns:
            The task id of the created task
        """
        spawn_id = None
        if isinstance(self.task_context, TaskContext):
            spawn_id = self.task_context.task_id
        self.send_message(
            ("add_task", (func, set(depends_on) if depends_on else None, spawn_id))
        )
        res_task_id = self.recv_message()
        self.to_commit.append(res_task_id)
        print("Created task id", res_task_id)
        return res_task_id

    def commit(self):
        self.send_message(("commit_task", tuple(self.to_commit)))
        _ = self.recv_message()
        print("Committed ", self.to_commit)
        self.to_commit = []
        return

    def start(self):
        self.task_context.to_server.put_nowait(("start", ()))

    def wait(self):
        self.task_context.to_server.put_nowait(("wait", ()))
        _ = self.task_context.from_server.get()

    def add_promise(self, promise: ConstrainedPromise):
        self.task_context.to_server.put_nowait(("add_promise", (promise,)))
        _ = self.task_context.from_server.get()


def run(root_task: Callable):
    wf_client = Workflow.create_server()
    _ = wf_client.add_task(root_task)
    wf_client.commit()
    wf_client.start()
    return wf_client
