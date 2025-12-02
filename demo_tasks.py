import time
import ork 

def t1(tctx: ork.TaskContext):
    print("hello from t1")

def t2(tctx: ork.TaskContext):
    print("hello from t2")

def t3(tctx: ork.TaskContext):
    print("hello from t3")

def cond_task(tctx: ork.TaskContext):
    print("hello from cond_task")
    return False

def t5_dep(tctx: ork.TaskContext):
    print("hello from t5_dep")
    time.sleep(1)

def t4(tctx: ork.TaskContext):
    wf = ork.WorkflowClient(tctx)
    print("hello from t4")
    t5_dep_task = wf.add_task(t5_dep)
    cond_task_id = wf.add_task(cond_task)
    wf.add_task(t5,depends_on=[ork.FromEdge(t5_dep_task,cond_task_id)])    
    wf.commit()
    return False


def t5(tctx: ork.TaskContext):
    print("hello from t5")
    wf = ork.WorkflowClient(tctx)
    # wf.add_constraint(OrkConstraintApp(1, orkcnstr.Atom(1, 2)))

def main_test():
    wf = ork.Workflow.create_server()

    task1 = wf.add_task(t1)
    task2 = wf.add_task(t2, depends_on=[ork.FromEdge(task1)])
    task3 = wf.add_task(t3)
    wf.add_task(t4, depends_on=[ork.FromEdge(task1), ork.FromEdge(task3)])
    wf.commit()
    wf.start()
    wf.join()