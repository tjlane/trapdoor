#!/usr/bin/env python

import time
import numpy as np
from mpi4py import MPI

COMM = MPI.COMM_WORLD
MPI_RANK = COMM.Get_rank()
MPI_SIZE = COMM.Get_size()
STATUS = MPI.Status()

def main():
    
    running = True
    
    if MPI_RANK == 0: # master
    
        buf = np.zeros(10)
        req = False
        
        while running:
            if req:
                req.Wait()
            req = COMM.Irecv(buf, source=MPI.ANY_SOURCE)
            print 'got message:', buf
            
            
            
    else:
        req = None
        while True:
            d = np.ones(10) * MPI_RANK
            time.sleep(np.random.rand() * 10.0)
            print "%d sending" % MPI_RANK
            if req:
                req.Wait()
            req = COMM.Isend(d, dest=0)
    
    return
    
    
if __name__ == '__main__':
    main()