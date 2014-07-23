
"""
Really these are just random functions here temporarially.
"""


import numpy as np


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


def binarize(psana_event, thresholds):
    """
    Threshold an image, setting values to +/- 1, for above/below the
    threshold value, respectively.

    Parameters
    ----------
    psana_event : psana.Event
        A psana event to extract images from and threshold

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