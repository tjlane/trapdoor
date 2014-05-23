#!/usr/bin/env python


"""
This file contains all code relating to the "guardian" MPI process
that actually processes data in real time and operates the shutter.

to do list
----------
-- figure out `shmemstring`
-- implement the Shutter class
-- allow xtc data access (single worker proc)
-- docstringzzzz

>> TJL May 2014
"""


import sys
sys.path.insert(1,'/reg/common/package/mpi4py/mpi4py-1.3.1/install/lib/python')
from mpi4py import MPI
import numpy as np

import psana


def camera_datatypes(camera_name):
    
    d = {'name' : 'type',
         'CxiDsd.0:Cspad.0' : psana.ndarray_float32_3,
         'CxiDs1.0:Cspad.0' : psana.ndarray_float32_3}
    
    if camera_name in d.keys():
        return d[camera_name]:
    else:
        raise KeyError('No known data type for camera: %s' % camera_name)
        return


class Shutter(object):
    
    def __init__(self):
        return
        
    @property
    def status(self):
        return 'open'
        
    def open(self):
        pass
        
    def close(self):
        pass
        



class Thresholder(object):
    
    def __init__(self, adu_threshold, camera_datatype, camera_src):
        
        self.adu_threshold   = adu_threshold
        self.camera_datatype = camera_datatype
        self.camera_src      = camera_src
        
        self._current_binary_image = np.zeros((32, 185, 388), np.int32)
        
        return
    
        
    def __call__(self, event_stream):
        
        not_done = True
        while not_done:
            
            evt = event_stream.next()
            
            evt.get(self.camera_datatype, self.camera_src)
            self._current_binary_image = self.binarize(image)
        
        return
        
    @property
    def current_binary_image(self):
        return self._current_binary_image
        
    
    def binarize(self, image):
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
        
        above_indicies = (image > self.adu_threshold)
        image[above_indicies] = 1
        image[np.logical_not(above_indicies)] = -1
        
        image = image.as_dtype(np.int32)
        
        return image

        
def main(camera_name, adu_threshold, consecutive_threshold, area_threshold):
    
    # setup MPI
    comm = MPI.COMM_WORLD
    mpi_rank = comm.Get_rank()
    mpi_size = comm.Get_size()
    
    
    # this is the "master" process
    if mpi_rank == 0: 
        
        # initialize shutter & map of camera damage
        beamline_shutter = Shutter()
        camera_damage = np.zeros((32, 185, 388), dtype=np.int32)
        
        # sit and collect thresholded images from the worker processes
        # we look for pixels that have many values above threshold in a short
        # period of time -- these guys will have a large positive values in
        # the `camera_damage` array
        while True:
            
            if beamline_shutter.status == 'open':
                
                comm.Reduce(camera_damage, thdr.consecutive_threshold, 
                            op=MPI.sum, root=0)
                        
                if np.sum( camera_damage > consecutive_threshold ) > area_threshold:
                    beamline_shutter.close()
                    print '/n *** THRESHOLD EXCEEDED -- CLOSING SHUTTER *** \n'
                
                camera_damage[camera_damage < 0] = 0
                
            else:
                print 'shutter closed, awaiting re-open...'
                
            except KeyboardInterrupt as e:
                print "recieved sigterm, shutting down all processes..."
                comm.Abort(1) # eeek! should take everything down immediately
                break 
        
        
        
    # this is a "worker" process
    else:
        
        camera_src = Source('DetInfo(%s)' % camera_name)
        camera_datatype = camera_datatypes(camera_name)
        thdr = Thresholder(adu_threshold, camera_datatype, camera_src)
        
        # lock onto the shared memory
        # WUT?
        shmemstring = "shmem=0_" + map[rank/8] + "_psana_SXR." + str(rank%8) + ":stop=no"
        ds = psana.DataSource(shmemstring)
        event_stream = ds.events()
        
        # this is an infinite loop
        thdr(event_stream)
        
        
    return

        
if __name__ == '__main__':
    
    camera_name           = 'CxiDs1.0:Cspad.0'
    adu_threshold         = 5000
    consecutive_threshold = 5
    area_threshold        = 30
    
    main(camera_name, adu_threshold, consecutive_threshold, area_threshold)
    

