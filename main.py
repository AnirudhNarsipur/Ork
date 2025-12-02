from ork import Workflow,TaskContext,WorkflowClient
# from constraints import OrkConstraintApp
import constraints as orkcnstr
import logging
import sys
# import flytekit as fl
def main():
    print("Hello from ork!")


def t1(tctx : TaskContext ):
    print("hello from t1")

# @register("t2")
def t2(tctx : TaskContext):
    print("hello from t2")

# @register("t3")
def t3(tctx : TaskContext):
    print("hello from t3")

# @register("t4")
def t4(tctx : TaskContext):
    wf = WorkflowClient(tctx)
    print("hello from t4")
    wf.add_task(t5)

def t5(tctx : TaskContext):
    print("hello from t5")
    # wf = WorkflowClient(tctx)
    # wf.add_constraint(OrkConstraintApp(1,orkcnstr.Atom(1,2)))

def main_test():
    wf : WorkflowClient = Workflow.create_server()
   
    task1 = wf.add_task(t1)
    task2 = wf.add_task(t2,depends_on=[task1])
    task3 = wf.add_task(t3)
    task4 = wf.add_task(t4,depends_on=[task1,task2])
    wf.start()



def setup_logger():
    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
if __name__ == "__main__":
   main_test()
