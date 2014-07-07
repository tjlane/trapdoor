#!/usr/bin/env python


"""
This file contains all code relating to the "guardian" MPI process
that actually processes data in real time and operates the shutter.
"""

import time

import psana
import epics
import numpy as np

from core import MapReducer

def camera_datatypes(camera_name):
    
    d = {'name' : 'type',
         'CxiDsd.0:Cspad.0' : psana.ndarray_float32_3,
         'CxiDs1.0:Cspad.0' : psana.CsPad.DataV2 #psana.ndarray_float32_3
        }
    
    if camera_name in d.keys():
        return d[camera_name]
    else:
        raise KeyError('No known data type for camera: %s' % camera_name)
        return


            
        
# this is the "map"

# need to add both cameras (!)
global camera_src
camera_src = psana.Source('DetInfo(CxiDs1.0:Cspad.0)')


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
    """

    cspad = psana_event.get(psana.CsPad.DataV2, camera_src)
    if not cspad:
        return np.zeros((32, 185, 388), dtype=np.int32)

    image = np.vstack([ cspad.quads(i).data() for i in range(4) ])
        
    above_indicies = (image > adu_threshold)
    image[above_indicies] = 1
    image[np.logical_not(above_indicies)] = -1
        
    image = image.astype(np.int32)
        
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
    Note that currently the shutter control is operated by two PVs, each
    mapping to a separate subroutine housed on the pulse-picker motor.

    So to open/close the shutter, we have to set separate to PVs to "1".
    
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

        self._close_routine_pv = epics.PV('CXI:ATC:MMS:29:S_CLOSE')
        self._open_routine_pv  = epics.PV('CXI:ATC:MMS:29:S_OPEN')

        self.debug_mode = debug_mode

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

        num_overloaded_pixels = np.sum( camera_damage_image > self.consecutive_threshold )
        #num_overloaded_pixels = np.sum( (camera_damage_image / self.consecutive_threshold).astype(np.int) )

        if num_overloaded_pixels > self.area_threshold:
            print ''
            print '*** THRESHOLD EXCEEDED -- SENDING SHUTTER CLOSE SIG ***'
            print '%d pixels over threshold (%d)' % (num_overloaded_pixels, self.area_threshold)
            print ''

        # NOTE: the status query is VERY slow right now -- ~10 seconds :(
        # may be faster on different machines tho
        #    if self.status == 'open': # dbl check
        #        print '/n *** THRESHOLD EXCEEDED -- SENDING SHUTTER CLOSE SIG *** \n'
        #        self.close()

        #print 'completed action (%.3f s)' % (time.time() - s)

        return
    

    @property
    def status(self):
        opn = self._close_routine_pv.status
        cls = self._open_routine_pv.status

        if opn and not cls:
            s = 'open'
        elif cls and not opn:
            s = 'closed'
        else:
            s = 'unknown'

        return s
        

    def close(self, timeout=5.0):
        """
        Shutter the beam.
        
        Parameters
        ----------
        timeout : float
            A timeout value in seconds.
        """

        print 'Sending signal to close: %s' % self._pv_str
        if not self.debug_mode:
            print '(in debug mode...)'
            self.pv.put(0)

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

        print 'Sending signal to open: %s' % self._pv_str
        if not self.debug_mode:
            print '(in debug mode...)'
            self.pv.put(1)

        start = time.time()
        while not self.status == 'closed':
            elapsed = time.time() - start
            if elapsed > timeout:
                print 'WARNING: shutter failed to open in %f seconds' % timeout
                return False

        print '... shutter opened'

        return True

        
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
    
    camera_buffer = np.zeros((32, 185, 388), dtype=np.int32)
    cntrl = ShutterControl(consecutive_threshold, area_threshold, debug_mode=True)
       
    monitor = MapReducer(binarize, accumulate_damage, cntrl,
                         result_buffer=camera_buffer)
    monitor.start(verbose=False)
    
    return

        
if __name__ == '__main__':
    
    # these are some default values for testing purposes only
    
    adu_threshold         = 5000
    consecutive_threshold = 5
    area_threshold        = 30
    
    run(adu_threshold, consecutive_threshold, area_threshold)
    

