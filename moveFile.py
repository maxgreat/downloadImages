import os
import glob
from os import mkdir
from os.path import basename, join, exists
import shutil
import multiprocessing
import time

mv = '/data/yfcc/*'
dest = '/data/yfcc2'

def producer(fToMove, queue):
    for i,f in enumerate(fToMove):
        if i % 10 == 1:
           print("%2.5f" % (i/len(fToMove)*100), '\%', end='\r')
        queue.put(f, block=True, timeout=None)

def consumer(args, queue):

    if queue.empty():
        time.sleep(0.1)
    
    while queue.qsize() > 0:
        f = queue.get(block=True, timeout=2.0)
        name = basename(f)
        dir = join(dest, name[:3])
        path = join(dir, name)
        
        if not exists(dir):
           mkdir(dir)
        
        if exists(path):
           print(path, ' already exists')
        else:
            try:
                shutil.move(f, path)
            except Exception:
                print("Cannot move :", f, 'to', path)
        if queue.empty():
            time.sleep(0.4)
    print('!!!!!!!!!!!!!  EXITING COSUMER  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
       
       
if __name__=='__main__':
    print('Reading repository')
    fileToMove = glob.glob(mv)
    print("There is :", len(fileToMove), " files to move")
    
    queue = multiprocessing.Queue(200000)

    processes = [
        multiprocessing.Process(target=producer, args=(fileToMove, queue))
    ]
    
    for i in range(30):
        processes.append(multiprocessing.Process(target=consumer, args=('a', queue)))
    print('There is :', len(processes), 'processes' )

    for p in processes:
        p.start()

    for p in processes:
        p.join()
