import multiprocessing as mp

def worker(q):
    q.put('hello')

if __name__ == '__main__':
    q = mp.Queue()
    p = mp.Process(target=worker, args=(q,))
    p.start()
    print('child pid', p.pid)
    print('got', q.get())
    p.join()
