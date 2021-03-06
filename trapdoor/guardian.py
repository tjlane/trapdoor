#!/usr/bin/env python

"""
This file contains all code relating to the "guardian" MPI process
that actually processes data in real time and operates the shutter.
"""

import os
import sys
import re
import time
import zmq
import datetime
import subprocess
import socket
import numpy as np


import psana
try:
    import epics
    EPICS_IMPORTED = True
except ImportError as e:
    print 'Could not import EPICS'
    EPICS_IMPORTED = False
    

from core import MapReducer, ShutdownInterrupt
from aux import which


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

        if EPICS_IMPORTED:
            self._control = epics.PV('CXI:R52:EVR:01:TRIG2:TPOL')
            self.debug_mode = debug_mode
        else:
            print 'No epics, so setting shutter control to debug mode'
            self._control = None
            self.debug_mode = True

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
        if self._control:
            if self._control.get():
                return 'open'
            else:
                return 'closed'
        else:
            return 'not_connected'
        

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

    Some documentation on the more cryptic objects contained herein

    pixel_counts:
    The pixel counts is a (threshold monitor X camera) array containing
    the number of pixels over threshold in an image.

    hitrate_buffer :
    The hitrate buffer is a (window size X threshold monitor X camera)
    array containing a one or zero indicating if each of the previous
    `window size` shots was a hit or not.

    history_buffer :
    A (history_size X threshold monitor X camera) array, containing the
    last `history_size` windows, averaged to compute a moving average
    hitrate. This is what gets plotted by the GUI.
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
        
        self.shutter_control = ShutterControl(10000, debug_mode=True)

        # set key parameters
        self._history_size = history_size
        self._window_size = window_size

        for m in monitors:
            self.add_monitor(*m)

        # manually init the MapReduce class
        self._source  = 'cxishmem'
        self.register_cfg_file('/reg/neh/home2/tjlane/opt/trapdoor/trapdoor/default.cfg') # todo 
        self._use_array_comm = True

        self.num_reduced_events = 0
        self._analysis_frequency = window_size

        self.init_zmq()

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
        
        if is_damage_control or (name == 'damage'):
            print 'Setting `%s` as damage control (adu : %s)' % (name, adu_threshold)
            self._damage_control_index = len(self._monitor_names) - 1
            self.shutter_control = ShutterControl(self._shutter_threshold)
        
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


    @property
    def _shutter_threshold(self):
        return self._adu_thresholds[self._damage_control_index]
    
    
    def set_monitor_thresholds(self, name, adu_threshold, area_threshold):
        
        # todo : may want to check threshold sanity

        mon_index = self.mon_index(name)

        self._adu_thresholds[mon_index]  = adu_threshold
        self._area_thresholds[mon_index] = area_threshold
        
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

    @property
    def _image_sizes(self):
        x = 32 * 185 * 388 # hardcoded for large CSPAD : todo
        return np.array([x, x])

    def mon_index(self, monitor_name):
        if monitor_name not in self.monitor_names:
            raise KeyError('Monitor: %s not registered yet, cannot update '
                           'threshold values' % monitor_name)
        return np.where( np.array(self.monitor_names) == monitor_name )[0][0]
        
        
    # ------------
    # COMMUNICATIONS
    #
    
    def init_zmq(self, pub_port=4747, recv_port=4748):
        """
        Initialize the ZMQ protocol, preparing the class for inter-process
        communication.
        """

        self._zmq_context = zmq.Context()
        #master_host = socket.gethostbyname_ex(self.stats['hosts'][0])[-1][0] # gives ip
        master_host = self.master_host

        topic = 'instructions'
        self._zmq_recv = self._zmq_context.socket(zmq.SUB)
        self._zmq_recv.connect('tcp://%s:%d' % (master_host, recv_port))
        self._zmq_recv.setsockopt(zmq.SUBSCRIBE, topic)

        if self.role == 'master':
            self._zmq_publish = self._zmq_context.socket(zmq.PUB)
            self._zmq_publish.bind('tcp://*:%d' % pub_port)

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
        stats['hitrates'] = self._history_buffer
        stats['shutter_control_threshold'] = self.shutter_control.threshold

        # add stats reported by the MapReduce class (hosts, etc)
        stats.update( super(CxiGuardian, self).stats )

        return stats
        

    def publish(self):
        """
        publish data widely (to monitors)
        """

        self._zmq_publish.send('stats', zmq.SNDMORE)
        self._zmq_publish.send_pyobj(self.stats)
        
        return


    def recv(self):
        """
        check for remote messages and take action
        """

        try:
            topic = self._zmq_recv.recv(zmq.NOBLOCK)
            instructions, content = self._zmq_recv.recv_pyobj(zmq.NOBLOCK)
            print 'recv msg: %s:%s' % (instructions, content)
        except zmq.Again as e:
            return # this means no new message
            
        # parse the message...
        if instructions == 'shutdown':
            self.shutdown(msg='remote process requested shutdown')
            
        elif instructions == 'set_parameters':
            self.set_parameters(content)

        elif instructions == 'set_prot_mode':
            self.shutter_control.debug_mode = not bool(content)
            
        else:
            raise RuntimeError('Remote message not understood: %s:%s' % (instructions, content))

        return
        

    def set_parameters(self, new_parameters):
        """
        Set the threshold parameters to new values.

        Parameters
        ----------
        new_parameters : dict
            A dict where the keys are "monitor :: {Area,Saturation} Threshold"
            and the values are the new parameter values.
        """

        for k,v in new_parameters.items():

            g = re.match('(\w+)\s+::\s+(\w+) [Tt]hreshold', k)

            name = g.group(1)
            i = self.mon_index(name)

            # todo : check threshold sanity

            p_type = g.group(2) # {Area, Saturation}
            if p_type == 'Area':
                self._area_thresholds[i] = v
            elif p_type == 'Saturation':
                self._adu_thresholds[i] = v
                if hasattr(self, '_damage_control_index'):
                    if i == self._damage_control_index:
                        print 'Updating damage control threshold'
                        self.shutter_control.threshold = v
            else:
                raise KeyError('No parameter type: %s. Must be one of'
                               ' {Area,Saturation}' % p_type)
            print 'Set %s %s threshold to: %s' % (self.monitor_names[i], p_type, str(v))
    
        return


    def extras(self):
        self.recv()
        return

        
    # ------------
    # MAP functionality
    # 
    
    def count_pixels_over_threshold(self, image, thresholds):
        """
        Count the number of pixels in `image` that are greater than each
        element of `thresholds`.

        Parameters
        ----------
        image : np.ndarray
            An image of any shape to threshold

        thresholds : np.ndarray
            A 1d array of thresholds

        Returns
        -------
        pixel_counts : np.ndarray
            An array the same shape and size as `thresholds`, where
            pixel_counts[i] contains the number of pixels in
            `image` greater than bins[i].
        """

        image = image.flatten()
        pixel_counts = np.zeros(len(thresholds), dtype=np.int)

        for i,t in enumerate(thresholds):
            pixel_counts[i] = np.sum( image > t )
        
        assert pixel_counts.shape[0] == len(thresholds), '%d %d' % \
                   (pixel_counts.shape[0], len(thresholds))

        #print thresholds, pixel_counts

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
        # 
        # NOTE : recall that area_thresholds are stored as %s, so e.g. "20"
        #        means that we need to evaluate it for 20% of the pixels...

        for i in range(self.num_monitors):
            num_px_thsd = int(self._area_thresholds[i] * self._image_sizes[0] * 0.01)
            hitrate_buffer[0,i,:] = (pixel_counts[i,:] > num_px_thsd )

        return hitrate_buffer
    
        
    # ------------
    # ACTION functionality
    #
    
    def _init_history_buffer(self):
        """
        The buffer is dimesion (N, M, 2):
        
            ( buffer length, num thresholds, number of cameras [ds1 & ds2] )
            
        """
        nm = self.num_monitors
        hs = self.history_size
        self._history_buffer = np.zeros((hs, nm, 2), dtype=np.int)
        return
    
        
    def action(self, hitrate_buffer):
        """
        Roll over the history buffer
        Communicate with the world
        """

        if self.num_reduced_events % self._analysis_frequency == 0:

            # compute the hitrate for the last few shots
            hitrate = np.mean(hitrate_buffer, axis=0)
        
            # insert hitrate values into the history buffer
            assert hitrate.shape == self._history_buffer.shape[1:]
            self._history_buffer = np.roll(self._history_buffer, 1, axis=0)
            self._history_buffer[0,:,:] = hitrate
            
            self.publish() 
        
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
    mpi_bin = which('mpirun')
    if mpi_bin == None:
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
    

