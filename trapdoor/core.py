#!/usr/bin/env python

"""
Core classes.
"""

import os
import sys
import re
import time
import socket
from glob import glob
import numpy as np

# currently the following line is necessary to ensure the correct mpi4py
# is used -- at a later date, this will be included in an ana release
# ---> if you don't include this line, you may get some cryptic error 
#      about OpenFiber not being able to alloc memory (!)
sys.path.insert(1,'/reg/common/package/mpi4py/mpi4py-1.3.1/install/lib/python')
from mpi4py import MPI

import psana


# --------------------------
# global vars (even tho they make me sad)

TYPE_MAP = {np.int     : MPI.INT,
            np.float32 : MPI.FLOAT,
            np.float64 : MPI.DOUBLE}
        
COMM = MPI.COMM_WORLD
MPI_RANK = COMM.Get_rank()
MPI_SIZE = COMM.Get_size()

#ERASE_LINE = '\x1b[1A\x1b[2K\x1b[1A'
ERASE_LINE = '\x1b[1A\x1b[2K'

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
        """
        Registers a psana configuration file at `path`.
        """
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
            
            node_number = MPI_RANK / 8
            core_number = MPI_RANK % 8

            # determine the multicast_mask by looking in /dev/shm
            shm_srvs = glob('/dev/shm/*psana*')
            if len(shm_srvs) == 1:
                shm_srv = shm_srvs[0]
            elif len(shm_srvs) == 0:
                raise IOError('Could not find a shm server access file in '
                              '/dev/shm on host: %s' % socket.gethostname())
            else:
                print 'WARNING: many shm server access files found: %s' % str(shm_srvs)
                shm_srv = shm_srvs[0]
                print 'using the first: %s' % shm_srv

            m = re.search('PdsMonitorSharedMemory_(\d+)_(\d+)_psana_CXI', shm_srv)
            if m == None:
                raise IOError('Could not find a monshmserver process on host: %s' % socket.gethostname())
            multicast_mask = int(m.groups()[1])
            
            # this was the old way, not so robust...
            #multicast_mask_map = [1, 2, 4, 8, 16, 32] # these are bits, one for each DSS Node
            #multicast_mask = multicast_mask_map[node_number]

            source_str = 'shmem=4_%d_psana_CXI.%d:stop=no' % (multicast_mask,
                                                              core_number)
                                                              
                                                              
        elif self.source in ['amoshmem', \
                             'sxrshmem', \
                             'xppshmem', \
                             'xcsshmem', \
                             'mecshmem']:
            raise NotImplementedError('Sorry, %s hasnt been implemented yet. '
                                      'Please contact tjlane <tjlane@stanford.edu>'
                                      ' or the current developer of this project'
                                      ' to request this functionality.')
            
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
            
        Optional Parameters
        -------------------
        result_buffer : np.ndarray
            If the `master` process must maintain a reduced result in memory,
            it is most efficient if this result is stored as an array. If
            buffer space is provided here (ie. an initialized array of the
            appropriate size & shape), it will be used for these purposes. A
            standard example might be if you were collecting an average CSPAD
            image, then you'd be storing that average in an array in the
            master process' memory, and you'd pass an empty (32, 188, 388)
            or similar array here.
            
        config_file : str
            The path to a psana configuration file that specifies upstream
            modules to apply to your data.
            
        source : str
            This specifies the data source you wish to access. Can either be
            shared memory, specified by the hutch followed by 'shmem' 
            (eg. 'cxishmem'), or an experiment identifier (eg. 'cxi4113').
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
        self._tachometer_run_freq    = 50
        self._tachometer_report_freq = 5

        return
    
        
    def start(self, tachometer=True, verbose=False):
        """
        Begin the map-reduce procedure.
        
        Optional Parameters
        -------------------
        tachometer : bool
            If `True`, print information about the speed at which data
            are being processed.
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
                #print 'Hello, from RANK %d' % MPI_RANK
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
                #print '%.2f || Master: %d events' % (time.time(), self.num_reduced_events)
                if req: req.Wait()
                req = irecv(self._buffer, source=MPI.ANY_SOURCE, tag=0)
                self._result = self.reduce(self._buffer, self._result)
                self.num_reduced_events += 1
                self.action(self._result)
                #self.check_for_stopsig()
                
                # this will get all the rate data from the workers and print it
                if tachometer:
                    if self.num_reduced_events % self._tachometer_run_freq == 0:
                        self.tachometer(verbose=True)
                
        return                  

    def stop(self):
        self._running = False
        
    # def check_for_stopsig(self):
        # todo : query remote process & if signal is there, stop()

    @property
    def running(self):
        return self._running
       
    @property 
    def result(self):
        return self._result
        
        
    def tachometer(self, verbose=True, inplace_text=True):
        """
        Gather the rate of data processing from all worker processes and, if
        `verbose`, display it.
        """

        # the first is better (no replacement), but not avail in numpy 1.6.X
        #sample = np.random.choice(np.arange(1, MPI_SIZE), min(MPI_SIZE, 8))
        sample = np.unique( np.random.randint(1, MPI_SIZE, min(MPI_SIZE, 8)) )

        rates = [ COMM.recv(source=i, tag=1) for i in sample ]
        mean_rate = np.mean(rates)
        total_rate = float(MPI_SIZE) * mean_rate
        
        if verbose:
            
            msg = [
            '>>           TACHOMETER',
            '-----------------------']
            for i in range(len(rates)):
                msg.append( 'Rank %d :: %.2f Hz' % (sample[i], rates[i]) )
            msg.extend([
            '-----------------------',
            'Mean:     %.2f Hz' % mean_rate,
            'Total:    %.2f Hz' % total_rate,
            '-----------------------',
            'Events processed: %d' % self.num_reduced_events,
            '-----------------------',
            ''])

            if not hasattr(self, '_printed_tachometer_buffer'):
                print '\n' * len(msg) #* 2
                self._printed_tachometer_buffer = True
            
            # if unique gets rid of lines, fill em back in
            num_missing_lines = min(MPI_SIZE, 8) - len(rates)

            if inplace_text:
                prefix = ERASE_LINE * (len(msg) + num_missing_lines + 1)
                prefix += '\n' * num_missing_lines
            else:
                prefix = ''
            msg = '\n'.join(msg)
            print msg,
        
        return
    

class Projector(object):

    # this code modified from pypad.utils.RadialAverager
    # github.com/tjlane/pypad

    def __init__(self, coordinate_values, mask, n_bins=101):
        """
        Initialize a projector object. This object projects images onto a 
        pre-specified axis.
        
        Parameters
        ----------
        coordinate_values : np.ndarray (float)
            A value for each pixel identifying it's value along the projection
            axis.
            
        mask : np.ndarray (int)
            A boolean (int) saying if each pixel is masked or not.
            
        n_bins : int
            The number of bins to employ. If `None` guesses a good value.
        """

        self.coordinate_values = coordinate_values
        self.mask = mask.astype(np.int)

        if not self.mask.shape == self.coordinate_values.shape:
            raise ValueError('mask/coordinate_values shape mismatch')

        self.n_bins = n_bins

        # figure out the number of bins to use
        if n_bins != None:
            self.n_bins = n_bins
            self._bin_factor = float(self.n_bins-0.5) / self.coordinate_values.max()
        else:
            self._bin_factor = 25.0
            self.n_bins = (self.coordinate_values.max() * self._bin_factor) + 1

        self._bin_assignments = np.floor( coordinate_values * self._bin_factor ).astype(np.int32)
        self._normalization_array = (np.bincount( self._bin_assignments.flatten(),
                                     weights=self.mask.flatten() ) \
                                     + 1e-100).astype(np.float)

        #print self.n_bins, self._bin_assignments.max() + 1 
        assert self.n_bins == self._bin_assignments.max() + 1, 'bin mismatch in init'
        self._normalization_array = self._normalization_array[:self.n_bins]

        return

    def __call__(self, image):
        """
        Project an image along an axis.

        Parameters
        ----------            
        image : np.ndarray
            The intensity at each pixel. Must be the same shape as 
            `coordinate_values`, the map of each pixel along the projection
            axis.

        Returns
        -------
        bin_values : ndarray, int
            The projected value in the bin.
        """

        image = image

        if not (image.shape == self.coordinate_values.shape):
            raise ValueError('`image` and `coordinate_values` must have the same shape')
        if not (image.shape == self.mask.shape):
            raise ValueError('`image` and `mask` must have the same shape')

        weights = image.flatten() * self.mask.flatten()
        bin_values = np.bincount(self._bin_assignments.flatten(), weights=weights)
        bin_values /= self._normalization_array

        assert bin_values.shape[0] == self.n_bins, 'bin number mismatch (%d, %d)' \
                                                   % (bin_values.shape[0], self.n_bins)

        return bin_values
    

    @property
    def bin_centers(self):
        return np.arange(self.n_bins) / self._bin_factor
    
    
