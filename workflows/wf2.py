import time
import ork
from datetime import datetime
import os 

CWD = os.getcwd()

def child1(tctx: ork.TaskContext,args):  
    time.sleep(5) # Simulate some work being done
    print("hello from child1, Got args:", args)
    return "child1"

def cond_task(tctx: ork.TaskContext,args):
    print("hello from cond_task, Got args:", args)
    # Marker 2: What if this is true?
    # return True
    return False 

def child2(tctx: ork.TaskContext,args):
    wf = ork.WorkflowClient(tctx) # Create a workflow client from the task context
    print("hello from child2 Got args:", args)
    if datetime.now().month == 12: # We only fire and forget in December
        wf.add_task(child_child2,depends_on=[ork.FromEdge(tctx.task_id)]) # Dynamically add a task t5 depending on t4. Will run after t4 completes
        wf.commit()
    time.sleep(0.1) # Simulate some work being done
    return "child2"

def evil_task(tctx: ork.TaskContext,args):
    print("hello from evil_task")

def child_child2(tctx: ork.TaskContext,args):
    print(f"hello from child_child2. Got args: {args}")
    # Marker 3: Dynamically spawn a task that depends on me even though it shouldn't
    # wf_client = ork.WorkflowClient(tctx)
    # wf_client.add_task(evil_task,depends_on=[ork.FromEdge(tctx.task_id)]) 
    # print("I: child_child2 spawned evil_task dynamically!")
    return "child_child2"
    

def root_task(tctx: ork.TaskContext,args):
    wf = ork.WorkflowClient(tctx) # Create a workflow client from the task context

    child1_id = wf.add_task(child1) # Add t2 as a task with no dependencies and returns a task id
    cond_task_id = wf.add_task(cond_task, depends_on=[ork.FromEdge(tctx.task_id)]) # Add t3 as a task depending on t2
    # Add t5 depending on t2 and a conditional dependency on t3 . The t3 dependency is only enforced if t4 returns True
    child2_id = wf.add_task(child2, depends_on=[ork.FromEdge(tctx.task_id), ork.FromEdge(child1_id,cond_task_id)])
    # No outgoing edges for any instance of t6
    wf.add_promise(ork.EdgePromise([child_child2],ork.All()) == 0)
    wf.commit() # Commit created tasks - marking them as ready for execution. If not committed, tasks will not run.
    return "root"
# Create a workflow and register some tasks statically
def main():
    wf_client = ork.create_server(workflow_id="wf2")
    wf_client.add_task(root_task)
    wf_client.commit()  
    # Marker 1: Run the workflow
    # wf_client.start()
    # wf_client.wait()

if __name__ == "__main__":
    main()