import time
import ork
from datetime import datetime
from typing import Optional


def ork_map(wf : ork.WorkflowClient,f,inp_ls : list,max_concurrent : Optional[int] = None):
    # Add a run promise if concurrency limit is specified
    if max_concurrent is not None:
        wf.add_promise(ork.RunPromise([f]) <= max_concurrent)
    # Add a promise that no tasks created by map can create child tasks
    wf.add_promise(ork.NodePromise([f],ork.All()) == 0)
    map_task_ids = []
    for inp in inp_ls:
        map_task_id = wf.add_task(f, args=(inp,))
        map_task_ids.append(map_task_id)
    # Commit the map tasks
    wf.commit()
    return map_task_ids

def secret_map_violation_task(tctx: ork.TaskContext,args):
    print("This task should not have been allowed to run!")
    return -1

def len_map_task(tctx: ork.TaskContext,args):
    # Marker 1: Map task violation
    # wf = ork.WorkflowClient(tctx) # Create a workflow client from the task context
    # wf.add_task(secret_map_violation_task) # This will violate the map promise
    # wf.commit()
    print("Running task id:  ", tctx.task_id)
    time.sleep(5) # Simulate some work being done
    return len(args["manual_args"][0])


def len_reducer_task(tctx: ork.TaskContext,args):
    final_val =  sum(args.values())
    print("Final reduced value is:", final_val)
    return final_val


def root_task(tctx: ork.TaskContext,args):
    wf = ork.WorkflowClient(tctx) # Create a workflow client from the task context

    # Map task can't create child tasks
    wf.add_promise(ork.NodePromise([len_map_task],ork.All()) == 0)

    rand_inputs = ["0" * i for i in range(1,11)]
    map_task_ids = ork_map(wf,len_map_task,rand_inputs,max_concurrent=5)

    reducer_id = wf.add_task(len_reducer_task, depends_on=[ork.FromEdge(tid) for tid in map_task_ids])
    wf.commit()

# Create a workflow and register some tasks statically
def main():
    wf_client = ork.create_server(workflow_id="wf3")
    wf_client.add_task(root_task)
    wf_client.commit()  
    wf_client.start()
    wf_client.wait()

if __name__ == "__main__":
    main()