

"""
Core classes for trapdoor
"""


import os
import sys
import abc
import numpy as np

import psana


TYPE_MAP = {np.int     : MPI.INT,
            np.float32 : MPI.FLOAT,
            np.float64 : MPI.DOUBLE}


try:
    from mpi4py import MPI
except ImportError as e:
    print 'Could not import mpi4py locally, looking for global version...'
    try:
        sys.path.insert(1,'/reg/common/package/mpi4py/mpi4py-1.3.1/install/lib/python')
        from mpi4py import MPI
        print '... success!'
    except:
        raise ImportError('Could not import mpi4py')
        
COMM = MPI.COMM_WORLD
MPI_RANK = COMM.Get_rank()
MPI_SIZE = COMM.Get_size()



class OnlinePsana(object):
    """
    Base class for any online psana app
    """
        
    @property
    def processes(self):

        raise NotImplementedError()
        
        if self.role == 'worker':
            pass
            
        elif self.role == 'master':
            pass

        return
        
    
    def shutdown(self):
        # likely not the best way to do this
        print "shutting down all processes..."
        try:
            MPI.Finalize()
        except Exception as e:
            print e
            comm.Abort(1) # eeek! should take everything down immediately
        return
    
        
    @property
    def cfg_file(self):
        if not hasattr(self, '_cfg_file'):
            return None
        else:
            return self._cfg_file
    
        
    def register_cfg_file(self, path):
        if not os.path.exists(path):
            raise IOError('Could not find configuration file: %s' % path)
        psana.setConfigFile(path)
        self._cfg_file = path
        return
    
        
    @property
    def role(self):
        if mpi_rank == 0:
            return 'master'
        else:
            return 'worker'
    
            
    @property
    def _source_string(self):
        raise NotImplementedError()
        shmem_string = "shmem=0_%d_psana_%s.%d:stop=no" % (i, hutch, j)
        return shmem_string
    
        
    @property
    def events(self):
        ds = psana.DataSource(self._source_string)
        return ds.events()
    
    
    
class MapReducer(OnlinePsana):
    """
    A class to perform flexible map-reduction from shared memory at LCLS.
    """
    
    
    def __init__(self, map_func, reduce_func, result_buffer=None, 
                config_file=None, thorough=False, source='shmem'):
                 
        """
        
        Parameters
        ----------
        map_func : function
            Function that takes a psana 'event' object and returns some value
            of interest.
            
        reduce_func : function
            Function that takes the output of `map_func`, as well as a
            previously stored result, and generates and updated solution. Note
            that powerful and flexible operations can be performed by
            using a class method here...
            
        result_buffer
        
        
        """
        
        self.register_cfg_file(config_file)
        
        self.map = map_func
        self.reduce = reduce_func
        self._buffer = None
        
        if result_buffer:
            self._use_array_comm = True
            self._result = np.zeros_like(result_buffer)
            self._buffer = result_buffer
            #self._comm_type = TYPE_MAP[init_result.dtype]
        else:
            self._use_array_comm = False
            
        self.num_events = 0   
        
        return
    
        
    def start(self):
        """
        Begin the map-reduce procedure
        """
        
        self._running = True
        
        # if we are communicating arrays, we can get some speedup by 
        # pre-allocating a result buffer and using the mpi4py array API
        
        if self._use_array_comm:
            
            if self.role == 'worker':
                req = None
                for evt in self.events:
                    result = self.map(evt)
                    if req: req.Wait() # be sure we're not still sending something
                    req = COMM.Isend(result, dest=0)
                
            elif self.role == 'master':
                self._buffer = np.zeros_like(self._result)
                while self.running:
                    COMM.Recv(self._buffer, source=MPI.ANY_SOURCE)
                    self._result = self.reduce(self._buffer, self._result)
                    self.num_events += 1
                    self.check_for_stopsig()
                    
        else:
        
            if self.role == 'worker':
                req = None
                for evt in self.events:
                    result = self.map(evt)
                    if req: req.Wait() # be sure we're not still sending something
                    req = COMM.isend(result, dest=0)
                
            elif self.role == 'master':
                while self.running:
                    buf = COMM.recv(source=MPI.ANY_SOURCE)
                    self._result = self.reduce(buf, self._result)
                    self.num_events += 1
                    self.check_for_stopsig()
                

    def stop(self):
        self._running = False
        
    def check_for_stopsig(self):
        pass
        # not implemented!
        # to do:
        # query remote process & if signal is there, stop()
        
    @property
    def running(self):
        return self._running
        
    def result(self):
        return self._result
        
    
    
    
    