#!/usr/bin/env python

"""
This file contains all code relating to the "guardian" MPI process
that actually processes data in real time and operates the shutter.
"""

import os
import time
import zmq
import datetime
import subprocess
import numpy as np

import psana
try:
    import epics
except ImportError as e:
    print 'Could not import EPICS'
    

from core import MapReducer, ShutdownInterrupt


class ShutterControl(object):

    
    def __init__(self, threshold, debug_mode=False):
        """
        Initialize a shutter control. This control detects damaged CSPAD events
        and, if enough damage (as specified by the user) is found, closes the
        fast shutter.
        
        Parameters
        ----------
        threshold : int
            The number of pixels on the CSPAD that must be damaged before tbe
            trigger to close the shutter is thrown
            
        debug_mode : bool
            If `True`, don't actually control the shutter, just print warning
            messages.
        """

        self.threshold = threshold
        self._control = epics.PV('CXI:R52:EVR:01:TRIG2:TPOL')
        self.debug_mode = debug_mode

        return
    

    def __call__(self, num_damaged_pixels):
        """
        Based on a CSPAD damage count, decide whether to shutter the beam.
        
        Parameters
        ----------
        num_damaged_pixels : int
            The number of damaged pixels on the CSPAD.
        """

        s = time.time()

        if num_damaged_pixels > self.threshold:
            print ''
            print '*** THRESHOLD EXCEEDED ***'
            print '%d pixels over threshold (%d)' % (num_damaged_pixels,
                                                     self.threshold)
            print ''

            if self.status == 'open':
                self.close()
            else:
                print 'Shutter already closed'

        return
    
        
    @property
    def status(self):
        if self._control.get():
            return 'open'
        else:
            return 'closed'
        

    def close(self, timeout=5.0):
        """
        Shutter the beam.
        
        Parameters
        ----------
        timeout : float
            A timeout value in seconds.
        """

        print 'SENDING CLOSE SIGNAL'
        if not self.debug_mode:
            self._control.put(0)
        else:
            print '\t(in debug mode, nothing done...)'

        start = time.time()
        while not self.status == 'closed':
            elapsed = time.time() - start
            if elapsed > timeout:
                print 'WARNING: shutter failed to close in %f seconds' % timeout
                return False

        print '... shutter closed'

        return True


    def open(self, timeout=5.0):
        """
        Open the beam shutter.

        Parameters
        ----------
        timeout : float
            A timeout value in seconds.
        """

        print 'Sending signal to open shutter'
        if not self.debug_mode:
            self.pv.put(1)
        else:
            print '\t(in debug mode, nothing done...)'

        start = time.time()
        while not self.status == 'closed':
            elapsed = time.time() - start
            if elapsed > timeout:
                print 'WARNING: shutter failed to open in %f seconds' % timeout
                return False

        print '... shutter opened'

        return True


class CxiGuardian(MapReducer):
    """
    Contains the message passing interface between shmem and the GUI.
    """
    
    
    _ds1_src = psana.Source('DetInfo(CxiDs1.0:Cspad.0)')
    _ds2_src = psana.Source('DetInfo(CxiDs2.0:Cspad.0)')
    
    
    def __init__(self, monitors=[], window_size=120, history_size=300):
        """
        Start an instance of the CXI CSPAD Guardian.
        
        Parameters
        ----------
        """

        # a "monitor" is defined by 3 parameters stored in order in the 
        # following lists
        self._monitor_names   = []
        self._adu_thresholds  = []
        self._area_thresholds = []
        
        self.shutter_control = lambda x : None # placeholder

        # set key parameters
        self._history_size = history_size
        self._window_size = window_size

        for m in monitors:
            self.add_monitor(*m)

        self._zmq_ready = 0

        # manually init the MapReduce class
        self._source  = 'cxishmem'
        #self.register_cfg_file('/reg/neh/home2/tjlane/opt/trapdoor/trapdoor/default.cfg') # todo
        self._use_array_comm = True

        self.num_reduced_events = 0
        self._analysis_frequency = 120

        return
    
        
    def start(self):
        if self.num_monitors > 0:
            super(CxiGuardian, self).start()
        else:
            raise RuntimeError('Need at least one monitor active before you can'
                               ' start the Guardian')
        return


    def add_monitor(self, name, adu_threshold, area_threshold, 
                    is_damage_control=False):

        print 'Rank %d : Adding monitor: %s (%d | %d)' % (self.MPI_RANK, name,
                                                          adu_threshold, area_threshold)
                    
        # todo : may want to check threshold sanity
        
        self._monitor_names.append(name)
        self._adu_thresholds.append(adu_threshold)
        self._area_thresholds.append(area_threshold)
        
        if is_damage_control:
            self.shutter_control = ShutterControl(adu_threshold)
            self._damage_control_index = len(self._monitor_names)
        
        # initialize buffers
        self._init_history_buffer()
        
        self._result = np.zeros(( self.window_size, 
                                  self.num_monitors,
                                  self.num_cameras ),
                                dtype=np.int)
        self._buffer = np.zeros(( self.num_monitors,
                                  self.num_cameras ),
                                dtype=np.int)

        return
    
    
    def set_monitor_thresholds(self, name, adu_threshold, area_threshold):
        
        # todo : may want to check threshold sanity
        
        if name not in self.monitor_names:
            raise KeyError('Monitor: %s not registered yet, cannot update '
                           'threshold values')
        else:
            mon_index = np.where( np.array(self.monitor_names) == name )[0][0]
        
        self._adu_thresholds[mon_index]  = adu_threshold
        self._area_thresholds[mon_index] = area_threshold
        
        return
    
    
    def set_damage_adu_threshold(self, threshold):
        if isinstance(self.shutter_control, ShutterControl):
            self.shutter_control.threshold = threshold
        else:
            raise TypeError('shutter control not initialized')
        return
    
    
    def set_damage_monitor(self, name):
        if name not in self.monitor_names:
            raise KeyError('Monitor: %s not registered yet, cannot update '
                           'threshold values')
        else:
            self._damage_control_index = int( np.where( np.array(self.monitor_names) == name )[0][0] )
        return
    

    @property
    def monitor_names(self):
        return self._monitor_names

    @property        
    def adu_thresholds(self):
        return self._adu_thresholds
        
    @property
    def area_thresholds(self):
        return self._area_thresholds
        
    @property 
    def num_monitors(self):
        return len(self.monitor_names)
        
    @property
    def num_cameras(self):
        return 2
        
    @property
    def history_size(self):
        return self._history_size
        
    @property
    def window_size(self):
        return self._window_size
        
    # ------------
    # COMMUNICATIONS
    #
    
    def init_zmq(self, pub_port=4747, pull_port=4748):
        """
        Initialize the ZMQ protocol, preparing the class for inter-process
        communication.
        """

        self._zmq_context = zmq.Context()

        self._zmq_pull = self._zmq_context.socket(zmq.PULL)
        self._zmq_pull.connect('tcp://127.0.0.1:%s' % pull_port)

        self._zmq_publish = self._zmq_context.socket(zmq.PUB)
        self._zmq_publish.bind('tcp://*:%s' % pub_port)

        self._zmq_ready = 1

        return
    
        
    @property
    def stats(self):
        """
        Return a dictoray of statistics about the running properties of the
        Guardian.
        """
        
        stats = {}
        
        # insert the values for some key parameters into stats
        properties = ['monitor_names',
                      'adu_thresholds',
                      'area_thresholds',
                      'num_monitors',
                      'num_cameras',
                      'history_size',
                      'window_size']
                      
        for p in properties:
            stats[p] = getattr(self, p)
            
        # also throw in specifics
        stats['hitrates'] = self._result
        
        if isinstance(self.shutter_control, ShutterControl):
            stats['shutter_control_threshold'] = self.shutter_control.threshold

        # add stats reported by the MapReduce class (hosts, etc)
        stats.update( super(CxiGuardian, self).stats )

        return stats
        

    def communicate(self):
        """
        Communicate with remote monitoring processes. This function:
        
            (1) Publishes statistics, broadcasting to any number of monitors
            (2) Checks for remote messages asking for a change of state
            
        Parameters
        ----------
        stats : dict
            Additional statistics to communicate. Should be keyed by a string
            describing the value.
        """

        if not self._zmq_ready:
           self.init_zmq()


        # ---- publish data widely (to monitors)
        print 'MASTER: sending results to GUI'
        self._zmq_publish.send('stats', zmq.SNDMORE)
        self._zmq_publish.send_pyobj(self.stats)
        
        
        # ---- check for remote messages and take action
        try:
            instructions, content = self._zmq_pull.recv_pyobj(zmq.NOBLOCK)
            print 'MASTER: recv msg: %s - %s' % (instructions, content)
        except zmq.Again as e:
            return # this means no new message
            
        # parse the message...
        if instructions == 'shutdown':
            self._shutdown(msg='remote process requested shutdown')
            
        elif instructions == 'set_parameters':
            # todo
            raise NotImplementedError('asking for param update')
            
        else:
            raise RuntimeError('Remote message not understood: %s' % msg)

        return
        
        
    def _shutdown(self, msg='cause unspecified'):
        """
        Throw an exception capable of taking down the entire MPI process.
        """
        raise ShutdownInterrupt('Calling for MPI shutdown :: %s' % msg)
        return
        
    # ------------
    # MAP functionality
    # 
    
    @staticmethod
    def digitize(x, bins, overwrite=True):
        """
        Similar to np.digitize, but faster, I hope. Bins must be monotonic.
        """

        # todo
        raise NotImplementedError('not debugged')

        if type(bins) == int:
            bins = [bins]
        
        if not overwrite:
            y = np.zeros(x.shape, dtype=np.int)
        else:
            y = x
        
        for i,b in enumerate(bins):
            y[ y > b] = i + 1
            if i == 0:
                y[y <= b] = 0
        
        if overwrite:
            y = y.astype(np.int)
        
        return y


    def count_pixels_over_threshold(self, image, bins):
        image = image.flatten()
        pixel_counts = np.bincount( np.digitize(image, bins) )[1:]
        assert pixel_counts.shape[0] == len(bins), '%d %d' % (pixel_counts.shape[0], len(bins))
        return pixel_counts
    
        
    def check_for_damage(self, pixel_counts):
        """
        Check for detector damage and shutter the beam if necessary
        """
        
        damaged_pixels = pixel_counts[self._damage_control_index,:]
        for dp in damaged_pixels:
           self.shutter_control(dp)
        
        return
    
        
    def map(self, psana_event):
        """
        Threshold an image, setting values to +/- 1, for above/below the
        threshold value, respectively.

        Parameters
        ----------
        psana_event : psana.Event
            A psana event to extract images from and threshold

        Returns
        -------
        pixel_counts : np.ndarray
            An N x 2 array. N is the number of thresholds. The first column is
            for the DS1 camera, the second is for DS2.
        """
        
        pixel_counts = np.zeros((self.num_monitors, self.num_cameras), dtype=np.int)
        
        # we need the thresholds in monotonic order for self.digitize
        # todo
        #threshold_order = np.argsort(self.adu_thresholds)
        #print self.adu_thresholds, threshold_order
        #b = self.adu_thresholds[threshold_order]
        
        b = self.adu_thresholds
        if type(b) == int: b = [b,] # needs to be iterable
        
        ds1 = psana_event.get(psana.CsPad.DataV2, self._ds1_src)
        ds2 = psana_event.get(psana.CsPad.DataV2, self._ds2_src)

        if not ds1:
            pass
        else:
            ds1_image = np.vstack([ ds1.quads(i).data() for i in range(4) ])
            pixel_counts[:,0] = self.count_pixels_over_threshold(ds1_image, b)

        if not ds2:
            pass
        else:
            ds2_image = np.vstack([ ds2.quads(i).data() for i in range(4) ])
            pixel_counts[:,1] = self.count_pixels_over_threshold(ds2_image, b)

        # put things back in their original order :: todo
        #reverse_map = np.argsort(threshold_order)
        #pixel_counts = pixel_counts[reverse_map,:]

        assert pixel_counts.shape == self._buffer.shape, 'buffer shape incorrect for map result'
        assert pixel_counts.dtype == self._buffer.dtype, 'buffer type incorrect for map result' 

        # check for damage
        if hasattr(self, '_damage_control_index'):
            self.check_for_damage(pixel_counts)
        
        return pixel_counts

    # ------------
    # REDUCE functionality
    

    def reduce(self, pixel_counts, hitrate_buffer):
        """
        Compute the if pixel_counts are a hit
        Rolls the hitrate buffer
        Stores the hitrate
        """

        assert hitrate_buffer.shape[1:] == pixel_counts.shape, '%s %s' \
                  % (str(hitrate_buffer.shape), str(pixel_counts.shape))
        
        # roll the buffer over, we'll replace the first entry in a moment
        hitrate_buffer = np.roll(hitrate_buffer, 1, axis=0)
        
        # for each data type (diffuse/xtal/damage), compute if this shot is
        # a hit or not (is hit if there are a sufficient number of pixels above
        # the "area" threshold) and store that value in a running buffer
        for i in range(self.num_monitors):
            hitrate_buffer[0,i,:] = pixel_counts[i,:] > self._area_thresholds[i]

        return hitrate_buffer
    
        
    # ------------
    # ACTION functionality
    #
    
    def _init_history_buffer(self):
        """
        The buffer is dimesion (N, M, 2):
        
            ( buffer length, num thresholds, number of cameras [ds1 & ds2] )
            
        """
        nt = self.num_monitors
        ws = self.history_size
        self._history_buffer = np.zeros((ws, nt, 2), dtype=np.int)
        return
    
        
    def action(self, hitrate_buffer):
        """
        Roll over the history buffer
        Communicate with the world
        """

        # compute the hitrate for the last few shots
        hitrate = np.mean(hitrate_buffer, axis=0)
        
        # insert hitrate values into the history buffer
        assert hitrate.shape == self._history_buffer.shape[1:]
        self._history_buffer = np.roll(self._history_buffer, 1, axis=0)
        self._history_buffer[0,:,:] = hitrate
        
        # periodically communicate the results
        if self.num_reduced_events % self._analysis_frequency == 0:
            self.communicate()
        
        return
        
        
def run(monitors):
    g = CxiGuardian(monitors)
    g.start()
    return


def run_mpi(monitors, hosts):
    """
    """
    
    # determine the total number of pixels implied by args.perc

    num_nodes = len(hosts)
    num_procs = num_nodes * 8
    host = hosts[0] # where the master resides
    hosts = [ h.strip() for h in hosts ]


    # give the user some output!
    print ''
    print '>>             TRAPDOOR'
    print '           ... starting'
    print '-----------------------'
    print 'nodes:         %d' % num_nodes
    print 'processors:    %d' % num_procs
    print 'hosts:         %s' % str(hosts)
    print '-----------------------' 


    # we have to jump through some hoops in order to make this work
    # I'm going to follow Chris' lead and write a bash script to disk
    # that gets called later by mpirun

    trapdoor_dir = os.path.join(os.environ['HOME'], '.trapdoor')
    if not os.path.exists(trapdoor_dir):
        os.mkdir(trapdoor_dir)
        print 'Created: %s' % trapdoor_dir


    # (1) write the script MPI will execute
    mpi_script_path = os.path.join(trapdoor_dir, 'mpi_script.sh')

    mpi_script_text = """#!/bin/bash

    # THIS IS AN AUTOMATICALLY GENERATED SCRIPT
    # CREATED BY: trapdoor
    # USER:       %s
    # DATE:       %s

    pyscript="from trapdoor import guardian; guardian.run(%s)"

    source /reg/g/psdm/etc/ana_env.sh
    . /reg/g/psdm/bin/sit_setup.sh

    python -c "$pyscript"

    """ % (os.environ['USER'],
           datetime.datetime.now(),
           str(monitors))

    f = open(mpi_script_path, 'w')
    f.write(mpi_script_text)
    f.close()

    # make that script chmod a+x
    st = os.stat(mpi_script_path)
    os.chmod(mpi_script_path, st.st_mode | 0111)

    print 'Wrote: %s' % mpi_script_path

    # (3) shell out the MPI command

    try:
        r = subprocess.check_call(['ssh', host, 'hostname'])
    except subprocess.CalledProcessError:
        raise IOError('No route to host: %s' % host)

    # try and find MPI
    lcls_mpi = '/reg/common/package/openmpi/openmpi-1.8/install/bin/mpirun'
    if os.path.exists(lcls_mpi):
        mpi_bin = '/reg/common/package/openmpi/openmpi-1.8/install/bin/mpirun'
    elif 'mpirun' in os.environ['PATH']:
        mpi_bin = 'mpirun'
    else:
        raise RuntimeError('Could not find an MPI `mpirun` executable!')

    cmd = [mpi_bin,
           '-n', str(num_procs),
           '--host', ','.join(hosts),
           mpi_script_path]

    print '-----------------------'
    print '>> starting MPI'
    print 'cmd: %s' % ' '.join(cmd)

    # for some reason, NOT passing the kwargs stdout/stderr allows the stdout
    # to reach the running terminal
    pipe = subprocess.Popen(cmd, shell=False)
    print '-----------------------'
    
    return


def test1():

    g = CxiGuardian()
    g.add_monitor('test_monitor', 5, 5)
    g._source = 'exp=cxia4113:run=30' # overwrite from shmem for testing
    g.start()

    return
    

def test2():
    run_mpi([('test_monitor', 5, 5)], ['daq-cxi-dss07'])
    return

        
if __name__ == '__main__':
    test2()
    

