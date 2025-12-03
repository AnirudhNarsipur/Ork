import time
import ork
from datetime import datetime
import os 
import random 

CWD = os.getcwd()

def secret_map_violation_task(tctx: ork.TaskContext,args):
    print("This task should not have been allowed to run!")
    return -1

def len_map_task(tctx: ork.TaskContext,args):
    # Marker 1: Map task violation
    # wf = ork.WorkflowClient(tctx) # Create a workflow client from the task context
    # wf.add_task(secret_map_violation_task) # This will violate the map promise
    # wf.commit()
    return len(args["manual_args"][0])


def len_reducer_task(tctx: ork.TaskContext,args):
    final_val =  sum(args.values())
    print("Final reduced value is:", final_val)
    return final_val


def root_task(tctx: ork.TaskContext,args):
    wf = ork.WorkflowClient(tctx) # Create a workflow client from the task context

    # Map task can't create child tasks
    wf.add_promise(ork.NodePromise([len_map_task],ork.All()) == 0)

    rand_inputs = [os.urandom(i) for i in range(1,6)]
    map_tasks = []
    for inp in rand_inputs:
        map_task_id = wf.add_task(len_map_task, args=(inp,))
        map_tasks.append(map_task_id)

    reducer_id = wf.add_task(len_reducer_task, depends_on=[ork.FromEdge(tid) for tid in map_tasks])
    wf.commit()

# Create a workflow and register some tasks statically
def main():
    wf_client = ork.create_server(workflow_id="wf2")
    wf_client.add_task(root_task)
    wf_client.commit()  
    wf_client.start()
    wf_client.wait()

if __name__ == "__main__":
    main()