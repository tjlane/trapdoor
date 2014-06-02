# script run with:
# /reg/common/package/openmpi/openmpi-1.8/install/bin/mpirun -n 24 --hostfile hostfile ./shm_sxr.sh

# shm_sxr.sh script was:
# #!/bin/bash
# . /reg/g/psdm/bin/sit_setup.sh ana-0.10.14
# python mpi_shm.py

# added these 3 lines to sxr.cnf:
#procmgr_config_mon.append({host:daq_sxr_mon01,  id:'monshmsrv_psana_1', flags:'s', cmd:pdsapp_path+'/monshmserver -p '+platform+' -P '+instrument+' -d -q 8 -u psana -i 36 -n 16 -s '+max_evt_sz })
#procmgr_config_mon.append({host:daq_sxr_mon02,  id:'monshmsrv_psana_2', flags:'s', cmd:pdsapp_path+'/monshmserver -p '+platform+' -P '+instrument+' -d -q 8 -u psana -i 18 -n 16 -s '+max_evt_sz })
#procmgr_config_mon.append({host:daq_sxr_mon03,  id:'monshmsrv_psana_3', flags:'s', cmd:pdsapp_path+'/monshmserver -p '+platform+' -P '+instrument+' -d -q 8 -u psana -i 9 -n 16 -s '+max_evt_sz })

# hostfile was:
# daq-sxr-mon03
# daq-sxr-mon02
# daq-sxr-mon01

from psana import *

import numpy as np
import sys
sys.path.insert(1,'/reg/common/package/mpi4py/mpi4py-1.3.1/install/lib/python')

from mpi4py import MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

import zmq
import sys
sys.path.append("./psanamon")
from psdata import ImageData

if rank==0:
    ####################
    # ZMQ SETUP 
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.setsockopt(zmq.SNDHWM, 10)
    socket.bind("tcp://*:%d" % 12321)
    ####################

map = []
map.append('9')
map.append('18')
map.append('36')

# this is not clean.  depends on the ordering in the mpi hostfile and on
# which multicast groups each monitoring node has signed up for
shmemstring = "shmem=0_" + map[rank/8] + "_psana_SXR." + str(rank%8) + ":stop=no"
events = DataSource(shmemstring).events()

src = Source('DetInfo(SxrBeamline.0:Opal1000.0)')

numgood=0
numbad=0
while 1:
    if (numgood+numbad)%10==0:
        print rank, numgood, numbad
    evt = events.next()

    frame = evt.get(Camera.FrameV1, src)
    if frame is None:
        numbad+=1
        continue

    if 'sum' in locals():
        sum+=frame.data16()
    else:
        sum=frame.data16().astype(int)
    
    numgood+=1

    if (numbad+numgood)%10 == 0:
        sumall = np.empty_like(sum)
        #sum the images across mpi cores
        comm.Reduce(sum,sumall)
        if rank==0:
            # display plot with: python psanamon/psplotclient.py -s daq-sxr-mon03 -p 12321 OPAL &
            myplot = ImageData(numbad+numgood, "opal image", sumall)
            socket.send("OPAL", zmq.SNDMORE)
            socket.send_pyobj(myplot)
