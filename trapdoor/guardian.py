#!/usr/bin/env python

"""
This file contains all code relating to the "guardian" MPI process
that actually processes data in real time and operates the shutter.
"""

import os
import time
import zmq

import psana
import epics
import numpy as np

from core import MapReducer


def camera_datatypes(camera_name):
    
    d = {'name' : 'type',
         'CxiDsd.0:Cspad.0' : psana.ndarray_float32_3,
         'CxiDs2.0:Cspad.0' : psana.ndarray_float32_3,
         'CxiDs1.0:Cspad.0' : psana.CsPad.DataV2 #psana.ndarray_float32_3
        }
    
    if camera_name in d.keys():
        return d[camera_name]
    else:
        raise KeyError('No known data type for camera: %s' % camera_name)
        return


# these globals are nasty, but should save a lot of time
global ds1_src
ds1_src = psana.Source('DetInfo(CxiDs1.0:Cspad.0)')

global ds2_src
ds2_src = psana.Source('DetInfo(CxiDs2.0:Cspad.0)')


def binarize(psana_event, adu_threshold=10000):
    """
    Threshold an image, setting values to +/- 1, for above/below the
    threshold value, respectively.
        
    Parameters
    ----------
    image : np.ndarray
        The image to threshold
            
    Returns
    -------
    thresh_image : np.ndarray
        Thresholded image
        
    Notes
    -----
    This is the 'map' function.
    """

    ds1 = psana_event.get(psana.CsPad.DataV2, ds1_src)
    ds2 = psana_event.get(psana.CsPad.DataV2, ds2_src)
    
    if not ds1:
        ds1_image = np.zeros((32, 185, 388), dtype=np.int32)
    else:
        ds1_image = np.vstack([ ds1.quads(i).data() for i in range(4) ])
        above_indicies = (ds1_image > adu_threshold)
        ds1_image[above_indicies] = 1
        ds1_image[np.logical_not(above_indicies)] = -1
        ds1_image = ds1_image.astype(np.int32)
            
    if not ds2:
        ds2_image = np.zeros((32, 185, 388), dtype=np.int32)
    else:
        ds2_image = np.vstack([ ds2.quads(i).data() for i in range(4) ])
        above_indicies = (ds2_image > adu_threshold)
        ds2_image[above_indicies] = 1
        ds2_image[np.logical_not(above_indicies)] = -1
        ds2_image = ds2_image.astype(np.int32)
    
    image = np.array((ds1_image, ds2_image))
    assert image.shape == (2, 32, 185, 388), 'img shp %s' % (str(image.shape),)
        
    return image


def accumulate_damage(new, old):
    """
    Accumulate a damage readout for the camera. This function 'reduces'
    binary images (where 1 = damage, 0 = fine) by counting consecutive damaged
    events on a per-pixel basis. The final returned image is roughly a count of
    the number of consecutive shots that are damaged.
    
    If we see a damaged pixel, that pixel gets a +1, otherwise it gets a -1. 
    No pixel can read below 0.
    
    Parameters
    ----------
    new : np.ndarray, binary
        The new data to add to the accumulator
        
    old : np.ndarray, binary
        The accumulator buffer
        
    Returns
    -------
    accumulated : np.ndarray, int
        A damage reading for each pixel
    
    Notes
    -----
    This is the 'reduce' function.
    """
    
    assert new.shape == old.shape, 'shape mismatch in reduce'

    x = new + old

    # same as : x[ x < 0 ] = 0, but faster
    x += np.abs(x)
    x /= 2

    return x


class ShutterControl(object):
    """    
    Notes
    -----
    This is the 'action' function.
    """

    
    def __init__(self, consecutive_threshold, area_threshold, debug_mode=False):
        """
        Initialize a shutter control. This control detects damaged CSPAD events
        and, if enough damage (as specified by the user) is found, closes the
        fast shutter.
        
        Parameters
        ----------
        consecutive_threshold : int
            The number of consecutive damaged events that must occur before the
            trigger to close the shutter is thrown
            
        area_threshold : int
            The number of pixels on the CSPAD that must be damaged before tbe
            trigger to close the shutter is thrown
            
        debug_mode : bool
            If `True`, don't actually control the shutter, just print warning
            messages.
        """

        self.consecutive_threshold = consecutive_threshold
        self.area_threshold = area_threshold

        self._control = epics.PV('CXI:R52:EVR:01:TRIG2:TPOL')

        self.debug_mode = debug_mode
        self._zmq_ready = 0 # don't use any ZMQ if not necessary

        return
    

    def __call__(self, camera_damage_image):
        """
        Based on a CSPAD image, decide whether to keep the shutter open or 
        closed.
        
        Parameters
        ----------
        camera_damage_image : np.ndarray, int
            An `accumulated` CSPAD image, where the pixel values indicate a
            running count of the number of damaged events at that pixel.
            
        See Also
        --------
        accumulate_damage : function
            The function that generates images that should be passed to this
            function.
        """

        s = time.time()

        self.num_overloaded_pixels = np.sum( camera_damage_image > self.consecutive_threshold )
        #num_overloaded_pixels = np.sum( (camera_damage_image / self.consecutive_threshold).astype(np.int) )

        if self.num_overloaded_pixels > self.area_threshold:
            print ''
            print '*** THRESHOLD EXCEEDED ***'
            print '%d pixels over threshold (%d)' % (self.num_overloaded_pixels,
                                                     self.area_threshold)
            print ''

            if self.status == 'open':
                self.close()
            else:
                print 'Shutter already closed'

        return


    def publish_stats(self, master_stats={}):
        """
        Publish statistics to a monitoring process.
        """

        #if not self._zmq_ready:
        #    self.init_zmq()

        #stats = {
        #         'ds1_damage'      : array, (self.num_overloaded_pixels)
        #         'ds2_damage'      : array, (self.num_overloaded_pixels)
        #         'xtal_hitrate'    : array,
        #         'diffuse_hitrate' : array
        #        }

        #stats.update(master_stats)
        #print 'from action:', stats
        #self._zmq_socket.send(stats)

        print 'publishing data to monitor...'

        return


    def init_zmq(self, port=4747):
        self._zmq_ready = 1
        self._zmq_context = zmq.Context()
        self._zmq_socket  = self._zmq_context.socket(zmq.PUB)
        self._zmq_socket.bind('tcp://*:%s' % port)
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


class GuardianMonitor(object):
    """
    Generates a visual display of the Guardian.
    """

    def __init__(self, port=4747):
        
        self._zmq_context = zmq.Context()
        self._zmq_socket  = self._zmq_context.socket(zmq.SUB)
        self._zmq_socket.connect('tcp://localhost:%s' % port)

        #self._app = QtGui.QApplication([])
        #pg.mkQApp()

        #self._widget = pg.PlotWidget()
        #self._widget.show()


        return

    def start(self):

        plot_buffer = []

        while True:
            stats = self._zmq_socket.recv()
            pg.plot(plot_buffer)
            pg.show()

        return


        
def run(adu_threshold, consecutive_threshold, area_threshold):
    """
    Run the CSPAD guardian. This starts up an infinite loop that looks for
    CSPAD damage and shutters the beam if it is found.
        
    Parameters
    ----------
    adu_threshold : int
        The ADU value that, if exceeded, identifies a pixel as damaged.
    
    consecutive_threshold : int
        The number of consecutive damaged events that must occur before the
        trigger to close the shutter is thrown
        
    area_threshold : int
        The number of pixels on the CSPAD that must be damaged before tbe
        trigger to close the shutter is thrown
    """
    
    camera_buffer = np.zeros((2, 32, 185, 388), dtype=np.int32)
    cntrl = ShutterControl(consecutive_threshold, area_threshold,
                           debug_mode=False)
       
    monitor = MapReducer(binarize, accumulate_damage, cntrl,
                         result_buffer=camera_buffer)
    monitor.start()
    
    return


def run_mpi(adu_threshold, consecutive_threshold, area_threshold,
            hosts):
    """
    Run the CSPAD guardian. This starts up an infinite loop that looks for
    CSPAD damage and shutters the beam if it is found.
        
    Parameters
    ----------
    adu_threshold : int
        The ADU value that, if exceeded, identifies a pixel as damaged.
    
    consecutive_threshold : int
        The number of consecutive damaged events that must occur before the
        trigger to close the shutter is thrown
        
    area_threshold : int
        The number of pixels on the CSPAD that must be damaged before tbe
        trigger to close the shutter is thrown
    """
    
    # determine the total number of pixels implied by args.perc


    num_procs = args.nodes * 8

    # give the user some output!
    print ''
    print '>>             TRAPDOOR'
    print '           ... starting'
    print '-----------------------'
    print 'nodes:         %d' % args.nodes
    print 'processors:    %d' % num_procs
    print 'host:          %s' % args.host
    print 'ADU tshd:      %d' % args.adu
    print 'Pixel # tshd:  %d' % area_threshold
    print 'Consec tshd:   %d' % args.consecutive
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

    pyscript="import sys; sys.path.append('/reg/neh/home2/tjlane/opt/trapdoor'); from trapdoor import guardian; guardian.run(%d, %d, %d)"

    source /reg/g/psdm/etc/ana_env.sh
    . /reg/g/psdm/bin/sit_setup.sh

    python -c "$pyscript"

    """ % (os.environ['USER'],
           datetime.datetime.now(),
           args.adu,
           args.consecutive,
           area_threshold)

    f = open(mpi_script_path, 'w')
    f.write(mpi_script_text)
    f.close()

    # make that script chmod a+x
    st = os.stat(mpi_script_path)
    os.chmod(mpi_script_path, st.st_mode | 0111)

    print 'Wrote: %s' % mpi_script_path

    # (3) shell out the MPI command

    try:
        r = subprocess.check_call(['ssh', args.host, 'hostname'])
    except subprocess.CalledProcessError:
        print 'RETURN CODE: %d' % r
        raise IOError('No route to host: %s' % args.host)

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
    r = subprocess.check_call(cmd, shell=False)
    print '-----------------------'
    
    return

        
if __name__ == '__main__':
    
    # these are some default values for testing purposes only
    
    adu_threshold         = 5000
    consecutive_threshold = 5
    area_threshold        = 30
    
    run(adu_threshold, consecutive_threshold, area_threshold)
    

