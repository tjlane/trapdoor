#!/usr/bin/env python


"""
This file contains all code relating to the "guardian" MPI process
that actually processes data in real time and operates the shutter.
"""


import psana
import epics
import numpy as np


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
    image = np.vstack([ cspad.quads(i).data() for i in range(4) ])
        
    above_indicies = (image > adu_threshold)
    image[above_indicies] = 1
    image[np.logical_not(above_indicies)] = -1
        
    image = image.astype(np.int32)
        
    return image


# reduce_func
def accumulate_damage(new, old):
    assert new.shape == old.shape, 'shape mismatch in reduce'
    x = new + old
    x[ x < 0 ] = 0
    return x


# action_func
class ShutterControl(object):
    
    def __init__(self, consecutive_threshold, area_threshold, pv=''):

        self.consecutive_threshold = consecutive_threshold
        self.area_threshold = area_threshold
        self.pv = epics.PV(pv)
        self._pv_str = pv

        return

    def __call__(self, camera_damage_image):
        """
        Based on a CSPAD image, decide whether to keep the shutter
        open or closed
        """

        if np.sum( camera_damage_image > self.consecutive_threshold ) > self.area_threshold:
            if self.status == 1: # dbl check
                print '/n *** THRESHOLD EXCEEDED -- SENDING SHUTTER CLOSE SIG *** \n'
                self.close()
        
        return

    @property
    def status(self):
        return self.pv.value

    def close(self):
        print 'Sending signal to close: %s' % self._pv_str
        self.pv.put(0)

    def open(self):
        print 'Sending signal to open: %s' % self._pv_str
        self.pv.put(1)


        
def main(camera_name, adu_threshold, consecutive_threshold, area_threshold,
         source='shmem'):
    
    camera_damage = np.zeros((32, 185, 388), dtype=np.int32)
    dd = DamageDecider(consecutive_threshold, area_threshold) 
       
    shmemstring = "shmem=4_1_psana_CXI.0:stop=no"

    monitor = MapReducer(binarize, accumulate_damage, dd,
                         result_buffer=camera_damage, source='shmem')
    monitor.start()
    
    return

        
if __name__ == '__main__':
    
    camera_name           = 'CxiDs1.0:Cspad.0'
    adu_threshold         = 5000
    consecutive_threshold = 5
    area_threshold        = 30
    
    main(camera_name, adu_threshold, consecutive_threshold, area_threshold)
    

