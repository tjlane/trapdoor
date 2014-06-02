#!/reg/neh/home2/tjlane/pytj/bin/python

import os
import sys
import time
import argparse
import h5py

import numpy as np
import psana 

# ---------------------------

#run = 46
run = int(sys.argv[1])

# ---------------------------
# acquire the data stream
print "\nReading run #: %d" % run
ds = psana.DataSource('exp=cxia4113:run=%d' % run)
epics = ds.env().epicsStore()
calib = ds.env().calibStore()

cspad_ds1_src  = psana.Source('DetInfo(CxiDs1.0:Cspad.0)')
cspad_ds2_src  = psana.Source('DetInfo(CxiDsd.0:Cspad.0)')
evr_src        = psana.Source('DetInfo(NoDetector.0:Evr.0)')
# ---------------------------


shot_index = 0
for evt in ds.events():

    cspad = evt.get(psana.CsPad.DataV2, cspad_ds1_src)
    image = np.vstack([ cspad.quads(i).data() for i in range(4) ])

    cdf = [ np.sum( image > t ) for t in range(1000, 10000, 1000) ]
    s = '%d\t' * (len(cdf) + 1)
    print s % tuple([shot_index] + cdf)

    shot_index += 1


