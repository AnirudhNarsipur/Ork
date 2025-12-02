from dataclasses import dataclass
from multiprocessing import Process, Queue

@dataclass
class Ctx:
    q: Queue

def f(ctx):
    print('child q', ctx.q)

if __name__ == '__main__':
    p = Process(target=f, args=(Ctx(Queue()),))
    p.start()
    # Parent exits immediately without join.
