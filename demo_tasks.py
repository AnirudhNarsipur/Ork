import time
import ork 
from datetime import datetime
import os 
CWD = os.getcwd()
def t1(tctx: ork.TaskContext):
    time.sleep(0.5) # Simulate some work being done
    print("hello from t1")

def t2(tctx: ork.TaskContext):  
    time.sleep(10) # Simulate some work being done
    print("hello from t2")

def t3(tctx: ork.TaskContext):
    print("hello from t3")
    return False # As false t4 will not wait for t2 to complete

def t4(tctx: ork.TaskContext):
    wf = ork.WorkflowClient(tctx) # Create a workflow client from the task context
    print("hello from t4")
    wf.add_task(t5,depends_on=[ork.FromEdge(tctx.task_id)]) # Dynamically add a task t5 depending on t4. Will run after t4 completes
    if datetime.now().month == 12: # We only fire and forget in December
        wf.add_task(fire_forget) # Dynamically spawn a fire-and-forget task that runs immediately once committed
    wf.commit()
    time.sleep(3) # Simulate some work being done
    return False

def fire_forget(tctx: ork.TaskContext):
    print("hello from fire_forget")

def t5(tctx: ork.TaskContext):
    print("hello from t5")
    


# Create a workflow and register some tasks statically
def main():
    wf = ork.Workflow.create_server() # Spawns a server process and returns a client to the server

    task1_id = wf.add_task(t1) # Add t1 as a task with no dependencies and returns a task id
    task2_id = wf.add_task(t2, depends_on=[ork.FromEdge(task1_id)]) # Add t2 as a task depending on t1
    task3_id = wf.add_task(t3) # Add t3 as a task with no dependencies
    # Add t4 depending on t1 and a conditional dependency on t2 . The t2 dependency is only enforced if t3 returns True
    wf.add_task(t4, depends_on=[ork.FromEdge(task1_id), ork.FromEdge(task2_id,task3_id)])
    # No outgoing edges for any instance of t5
    wf.add_promise(ork.OrkPromise(ork.FS([t5]),ork.NONE()) )
    wf.commit() # Commit created tasks - marking them as ready for execution. If not committed, tasks will not run.
    wf.start() # Start executing tasks in the workflow
    wf.join() # Block until all tasks have completed