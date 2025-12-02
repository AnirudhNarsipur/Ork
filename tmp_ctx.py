from dataclasses import dataclass
from multiprocessing import Process, Queue

@dataclass
class TaskContext:
    q1: Queue
    q2: Queue

def start_server(ctx: TaskContext):
    print('child context', ctx)

if __name__ == '__main__':
    ctx = TaskContext(Queue(), Queue())
    p = Process(target=start_server, args=(ctx,))
    p.start()
    p.join()
    print('done')
