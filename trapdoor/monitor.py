
import subprocess
import numpy as np


def start_guardian(hutch, num_nodes):
    """
    This is messy as crap -- I wonder if I can make it better!
    """
    
    # locate mpirun exec
    mpi_exec = '/reg/common/package/openmpi/openmpi-1.8/install/bin/mpirun'
    
    # write "hostname" file
    hostfile_path = '/tmp/td_h%d' % int(np.random.rand() * 1e16)
    txt = "".join(["daq-%s-mon%.2d\n" % (hutch, i) for i in range(num_nodes)])
           
    # run the MPI command
    cmd = [mpi_exec, '-n %d' % num_nodes*8, '--hostfile %s' % hostfile_path, 'run_trapdoor_guardian']
           
    print cmd
    subprocess.check_call(cmd, stdout=subprocess.PIPE)
    
    return