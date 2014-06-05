#!/usr/bin/env python

"""
Core classes for trapdoor
"""

import os
import sys
import time
import numpy as np

import psana


# ----------------------------
# import & setup MPI env

#try:
#    raise ImportError()
    #from mpi4py import MPI
#except ImportError as e:
#    print 'Could not import mpi4py locally, looking for global version...'
#    try:
#        sys.path.insert(1,'/reg/common/package/mpi4py/mpi4py-1.3.1/install/lib/python')
#        from mpi4py import MPI
#        print '... success!'
#    except:
#        raise ImportError('Could not import mpi4py')

sys.path.insert(1,'/reg/common/package/mpi4py/mpi4py-1.3.1/install/lib/python')
from mpi4py import MPI


TYPE_MAP = {np.int     : MPI.INT,
            np.float32 : MPI.FLOAT,
            np.float64 : MPI.DOUBLE}
        
COMM = MPI.COMM_WORLD
MPI_RANK = COMM.Get_rank()
MPI_SIZE = COMM.Get_size()

ERASE_LINE = '\x1b[1A\x1b[2K\x1b[1A'

# --------------------------


class OnlinePsana(object):
    """
    Base class for any online psana app
    """
    
    @property
    def source(self):
        return self._source
    
        
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
        if hasattr(self, '_role'):
            return self._role # this allows us to override for debugging...
        elif MPI_RANK == 0:
            return 'master'
        else:
            return 'worker'
    
            
    @property
    def _source_string(self):
        
        if self.source == 'cxishmem':
            
            multicast_mask_map = [1, 2, 4, 8, 16, 32] # these are bits, one for each DSS Node
            node_number = MPI_RANK / 8
            multicast_mask = multicast_mask_map[node_number]
            #multicast_mask = 8
            
            core_number = MPI_RANK % 8
            source_str = 'shmem=4_%d_psana_CXI.%d:stop=no' % (multicast_mask,
                                                              core_number)
            print 'RANK %d :: NODE %d :: %s' % (MPI_RANK, node_number, source_str)
            
        else:
            source_str = self.source
            
        return source_str
    
        
    @property
    def events(self):
        print 'Accessing data stream: %s' % self._source_string
        ds = psana.DataSource(self._source_string)
        return ds.events()
    
    
    
class MapReducer(OnlinePsana):
    """
    A class to perform flexible map-reduction from shared memory at LCLS.
    """
    
    
    def __init__(self, map_func, reduce_func, action_func,
                 result_buffer=None, config_file=None,
                 source='cxishmem'):
                 
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

        action_func : function
            Function that takes the reduced result and does something
            with it. Note that you can control computational cost using
            the `self.num_events` counter.
            
        result_buffer
        
        
        """
        
        self._source = source
       
        if config_file: 
            self.register_cfg_file(config_file)
        
        self.map = map_func
        self.reduce = reduce_func
        self.action = action_func
        self._buffer = None
        
        if result_buffer != None:
            self._use_array_comm = True
            self._result = np.zeros_like(result_buffer)
            self._buffer = result_buffer
        else:
            self._use_array_comm = False
            
        self.num_reduced_events = 0
        self._tachometer_run_freq    = 120
        self._tachometer_report_freq = 5

        return
    
        
    def start(self, tachometer=True, verbose=False):
        """
        Begin the map-reduce procedure
        """
        
        self._running = True
        
        # if we are communicating arrays, we can get some speedup by 
        # pre-allocating a result buffer and using the mpi4py array API
        
        if self._use_array_comm:
            isend = COMM.Isend
            recv  = COMM.Recv
            irecv = COMM.Irecv
        else:
            isend = COMM.isend
            recv  = COMM.recv
            irecv = COMM.irecv
            
        # enter loops for both workers and master
        
        # workers loop over events from psana
        if self.role == 'worker':
            if verbose: print 'Starting array-enabled worker (r%d)' % MPI_RANK

            req = None
            event_index = 0
            
            start_time = time.time()

            for evt in self.events:
                if not evt: continue

                result = self.map(evt)

                if not type(result) == np.ndarray and self._use_array_comm:
                    raise TypeError('Output of `map_func` must be a numpy array'
                                    ' if `result_buffer` is specified! Got: %s'
                                    '' % type(result))

                # send the mapped event data to the master process
                if req: req.Wait() # be sure we're not still sending something
                req = isend(result, dest=0, tag=0)
                event_index += 1

                # send the rate of processing to the master process
                if tachometer:
                    if event_index % self._tachometer_report_freq == 0:
                        rate = float(self._tachometer_report_freq) / (time.time() - start_time)
                        start_time = time.time()
                        COMM.isend(rate, dest=0, tag=1)
                        if verbose:
                            print 'RANK %d reporting rate: %.2f' % (MPI_RANK, rate)

                if verbose:
                    if event_index % 100 == 0:
                        print '%d evts processed on RANK %d' % (event_index, MPI_RANK)
                
           
        # master loops continuously, looking for communicated from workers & 
        #     reduces each of those
        elif self.role == 'master':
            if verbose: print 'Starting array-enabled master (r%d)' % MPI_RANK
            
            if self._use_array_comm:
                self._buffer = np.zeros_like(self._result)
           
            req = None 
            while self.running:
                print '%.2f || Master: %d events' % (time.time(), self.num_reduced_events)
                if req: req.Wait()
                req = irecv(self._buffer, source=MPI.ANY_SOURCE, tag=0)
                self._result = self.reduce(self._buffer, self._result)
                self.num_reduced_events += 1
                self.action(self._result)
                self.check_for_stopsig()
                
                # this will get all the rate data from the workers and print it
                if tachometer:
                    if self.num_reduced_events % self._tachometer_run_freq == 0:
                        self.tachometer(verbose=True)
                
        return                  

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
       
    @property 
    def result(self):
        return self._result
        
        
    def tachometer(self, verbose=True):
        """
        Gather the rate of data processing from all worker processes and, if
        `verbose`, display it.
        """
        sample = np.random.choice(np.arange(1, MPI_SIZE), min(MPI_SIZE, 8))
        rates = [ COMM.recv(source=i, tag=1) for i in sample ]
        mean_rate = np.mean(rates)
        total_rate = np.sum(rates)
        
        if verbose:
            
            msg = [
            '>>           shmem',
            '>>      TACHOMETER',
            '------------------']
            for i in range(1, MPI_SIZE):
                msg.append( 'Rank %d :: %.2f Hz' % (i, rates[i-1]) )
            msg.extend([
            '------------------',
            'Mean:      %.2f Hz' % mean_rate,
            'Total:     %.2f Hz' % total_rate,
            '------------------',
            ''])

            if not hasattr(self, '_printed_tachometer_buffer'):
                print '\n' * len(msg) * 4
                self._printed_tachometer_buffer = True
            
            prefix = ERASE_LINE * len(msg)
            msg = prefix + '\n'.join(msg)
            print msg,
        
        return
        
    
    
    
    
