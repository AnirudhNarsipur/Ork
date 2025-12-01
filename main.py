from ork import register,Workflow
import logging
import sys
def main():
    print("Hello from ork!")


# @register
def t1():
    print("hello from t1")

# @register
def t2():
    print("hello from t2")

def t3():
    print("hello from t3")

def t4():
    print("hello from t4")

def main_test():
    wf = Workflow()
    task1 = wf.add_task(t1)
    task2 = wf.add_task(t2,depends_on=[task1])
    task3 = wf.add_task(t3)
    task4 = wf.add_task(t4,depends_on=[task1,task2])
    wf.execute()


def setup_logger():
    logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
if __name__ == "__main__":

    main_test()
