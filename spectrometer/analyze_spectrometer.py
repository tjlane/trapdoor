#!/usr/bin/env python

import sys
import time

import numpy as np
from matplotlib import pyplot as plt

import epics
import psana

sys.path.append('/reg/neh/home2/tjlane/opt/trapdoor')
from trapdoor import core


"""
Code to analyze the FEL energy spectrum from CXI DG3.
"""

# this code modified from pypad.utils.RadialAverager
# github.com/tjlane/pypad

class Projector(object):
    
    def __init__(self, coordinate_values, mask, n_bins=101, normalize=False):
        """
        Parameters
        ----------
        coordinate_values : np.ndarray (float)
            For each pixel, this is the momentum transfer value of that pixel
        mask : np.ndarray (int)
            A boolean (int) saying if each pixel is masked or not
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
            #self._bin_factor = float(self.n_bins-0.5) / self.coordinate_values.max()
        else:
            #self._bin_factor = 25.0
            self.n_bins = (self.coordinate_values.max() * 25.0) + 1 # default
       
        coordinate_values -= coordinate_values.min() 
        coordinate_values /= coordinate_values.max()
        self._bin_assignments = np.floor( coordinate_values * (self.n_bins-1) ).astype(np.int32)

        if normalize:
            self._normalization_array = (np.bincount( self._bin_assignments.flatten(),
                                                      weights=self.mask.flatten() ) \
                                                              + 1e-100).astype(np.float)
        else:
            self._normalization_array = np.ones(self.n_bins)

        assert self.n_bins == self._bin_assignments.max() + 1, 'bin mismatch in init'
        self._normalization_array = self._normalization_array[:self.n_bins]
        
        return
    
    def __call__(self, image):
        """
        Bin pixel intensities by their momentum transfer.
        
        Parameters
        ----------            
        image : np.ndarray
            The intensity at each pixel, same shape as pixel_pos


        Returns
        -------
        bin_centers : ndarray, float
            The q center of each bin.

        bin_values : ndarray, int
            The average intensity in the bin.
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

    #@property
    #def bin_centers(self):
    #    return np.arange(self.n_bins) / self._bin_factor


class SpectrometerAnalyzer(object):

    def __init__(self, n_bins=101, pulse_energy_bins=np.linspace(0.0, 3.0, 100)):
        
        self.shots_processed = 0
        
        # ---- EPICS PVs
        self.rotation_angle = epics.caget('CXI:EXS:CNTL.H')
        
        
        # 
        self._hst_data = epics.PV('CXI:EXS:HST:01:data')
        self._hst_x    = epics.PV('CXI:EXS:HST:01:x')
        self._hst_y    = epics.PV('CXI:EXS:HST:01:y')
        
        # Scaling parameters of CXI spectrometer
        # Values based on calibration with XPP mono
        self._exs_dEdy = epics.caget('CXI:EXS:CNTL.E')
        self._exs_E0   = epics.caget('CXI:EXS:CNTL.F')
        self._exs_y0   = epics.caget('CXI:EXS:CNTL.G')
        
        #hst_xaxis = self._exs_E0 + np.arange(nx) * exs_dEdy
        
        # Calibrated spectrometer energy -- 1024 elements
        
        
        # ---- psana data sources
        self.dg3_src    = psana.Source('DetInfo(CxiDg3.0:Opal1000.0)')
        self.ebeam_src  = psana.Source('BldInfo(EBeam)')
        self.gasdet_src = psana.Source('BldInfo(FEEGasDetEnergy)')
        
        # ---- create a projector function to map DG3 images to energy spectra
        self.n_bins = n_bins
        self.projector = self._init_projector()
        
        return

    def map(self, psana_event):

        camdata = psana_event.get(psana.Camera.FrameV1, self.dg3_src)
        ebeam   = psana_event.get(psana.Bld.BldDataEBeamV5, self.ebeam_src)
        gasdet  = psana_event.get(psana.Bld.BldDataFEEGasDetEnergy, self.gasdet_src)
        eventid = psana_event.get(psana.EventId)

        try:
            #print '\r' + ebeam.ebeamL3Energy(), gasdet.f_11_ENRC(), eventid.time()[1], eventid.fiducials(),
            energy_spectrum = self.projector(camdata.data16())
        except:
            print '--- corrupt shot ---'
            energy_spectrum = np.zeros(self.n_bins)
        
        return energy_spectrum


    def reduce(self, new, old):
        
        self.shots_processed += 1
        
        # perform a running average
        sp = float(self.shots_processed)
        average_spectrum = old.astype(np.float) * ( (sp-1) / sp ) + new.astype(np.float) / sp

        if self.shots_processed % 10 == 0:
            ax.cla()
            ax.set_xlabel('Energy (arb)')
            ax.set_ylabel('Intensity (ADU)')
            ax.plot(average_spectrum, color='b')
            ax.plot(new, color='g')
            plt.draw() 
        
        
        return average_spectrum


    # action function
    def store(self, average_spectrum):

        #self._hst_data.put(average_spectrum)

        return
        
        
    def _init_projector(self):
        
        image_shape = (1024, 1024)
        #assert np.product(image_shape) == self._hst_data.get().size
        
        # compute the orthogonal projection of each pixel's position onto
        # a unit vector "s" defined by the rotation angle set
        
        s = np.array([1, np.tan(self.rotation_angle)])
        s /= np.linalg.norm(s)
        
        pixel_coords = np.meshgrid( np.arange(image_shape[0]),
                                    np.arange(image_shape[1]) )
                                    
        # dot product v-dot-s for each pixel
        projection_values = pixel_coords[0] * s[0] + pixel_coords[1] * s[1]
        
        # this could become more sophisticated later...
        mask = np.ones(image_shape)
        
        prj = Projector(projection_values, mask, n_bins=self.n_bins)
        
        return prj


def main():

    if core.MPI_RANK == 0:
        plt.ion()
        plt.figure()
        global ax
        ax = plt.subplot(111)
    
    n_bins = 101
    
    spctrm_ana = SpectrometerAnalyzer(n_bins=n_bins)
    buf = np.empty(n_bins)
    
    monitor = core.MapReducer(spctrm_ana.map,
                              spctrm_ana.reduce,
                              spctrm_ana.store,
                              result_buffer=buf,
                              source='cxishmem')
    monitor.start(verbose=False)
    
    return
    
    
if __name__ == '__main__':
    main()
    
    
