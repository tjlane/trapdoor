# ipython --pylab

from pylab import * 
from epics import PV
from psana import *
import re
import time
from glob import glob
from scipy import ndimage

exs = PV('CXI:EXS:HISTP')

MPI_RANK = 0
shm_srvs = glob('/dev/shm/*psana*')
shm_srv = shm_srvs[0]
m = re.search('PdsMonitorSharedMemory_(\d)_(\d)_psana_CXI', shm_srv)
multicast_mask = int(m.groups()[1])
node_number = MPI_RANK / 8
core_number = MPI_RANK % 8
source_str = 'shmem=4_%d_psana_CXI.%d:stop=no' % (multicast_mask,core_number)

events = DataSource(source_str).events()
src = Source('DetInfo(CxiDg3.0:Opal1000.0)')
src_ebeam = Source('BldInfo(EBeam)')
src_gasdet = Source('BldInfo(FEEGasDetEnergy)')

rotangle = PV('CXI:EXS:CNTL.H').get()
hst = PV('CXI:EXS:HST:01:data')
xhst = PV('CXI:EXS:HST:01:x')
yhst = PV('CXI:EXS:HST:01:y')
nhst = hst.get().size
nx = 1024
ny = 128

# Scaling parameters of CXI spectrometer
# Values based on calibration with XPP mono
exs_dEdy = PV('CXI:EXS:CNTL.E').get()
exs_E0 = PV('CXI:EXS:CNTL.F').get()
exs_y0 = PV('CXI:EXS:CNTL.G').get()
exs_rotangle = PV('CXI:EXS:CNTL.H').get()
hst_xaxis = exs_E0+arange(nx)*exs_dEdy
# Calibrated spectrometer energy -- 1024 elements 
xhst = PV('CXI:EXS:HST:01:x')
# Output calibrated spectrum to PV
xhst.put(hst_xaxis)

evt = events.next()
rimg = ndimage.rotate(evt.get(Camera.FrameV1, src).data16(),rotangle)
prof = rimg.sum(axis=1)
nprof = prof.size
ka = (nprof-nx)/2
kb = nprof-ka-1
kx0 = arange(8)*ny

nloop = ny

#for j in range(5):
j = 0
while True:
    time0 = time.time()
    data = empty(nx*ny)
    edata = empty(nx)
    for i in range(nloop):
        print 'hist loop', i
        evt = events.next()
#        print 'get event', i+j*nloop
        camdata = evt.get(Camera.FrameV1, src)
        ebeam = evt.get(Bld.BldDataEBeamV5, src_ebeam)
        gasdet = evt.get(Bld.BldDataFEEGasDetEnergy, src_gasdet)
        eventid = evt.get(EventId)
        if camdata != None:
            rimg = ndimage.rotate(camdata.data16(),rotangle,order=0)
            prof = rimg.sum(axis=1)
            data[i*nx:(i+1)*nx] = prof[ka:kb]
        if ebeam != None:
            edata[i] = ebeam.ebeamL3Energy()
            edata[i+ny] = ebeam.ebeamEnergyBC1()
            edata[i+2*ny] = ebeam.ebeamEnergyBC2()
        if gasdet != None:
            edata[i+3*ny] = gasdet.f_11_ENRC()
            edata[i+4*ny] = gasdet.f_12_ENRC()
        if eventid != None:
            edata[i+5*ny] = eventid.time()[1] 
            edata[i+6*ny] = eventid.fiducials() 
            edata[i+7*ny] = eventid.ticks()
        if i % 8 == 0:
            print time.time()-time0,i,j,edata[i+kx0]
    yhst.put(edata)
    time.sleep(0.5)
    hst.put(data)
    plt.imshow(data.reshape(ny,nx))
    j += 1


