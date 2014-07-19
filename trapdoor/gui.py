
"""
Contains the implementation of the GUI used to monitor and control trapdoor
"""

import sys
import zmq
import numpy as np

from pyqtgraph.Qt import QtGui, QtCore
import pyqtgraph as pg
from pyqtgraph.parametertree import Parameter, ParameterTree

#from trapdoor import guardian


class TrapdoorWidget(QtGui.QWidget):
    """
    Provides methods both for launching trapdoor and looking at feedback from
    launched processes.
    """
    
    def __init__(self):
        
        super(TrapdoorWidget, self).__init__()
        self.setWindowTitle('Trapdoor: CSPAD Monitoring')
        self._init_zmq()
        self._draw_canvas()
        
        return
    
        
    @property
    def monitor_status(self):
        #raise NotImplementedError()
        # should return one of: 'ready', 'running', 'unknown', ...
        return 'ready'
        
        
    @property
    def hosts(self):
        """
        Returns a list of the hosts being used to run.
        """
        s = str(self._host_text_widget.toPlainText())
        l = s.split(',')
        return l
    
        
    @property
    def parameters(self):
        """
        Return a dictionary of the parameter values currently set.
        """
        od = self._params.getValues()
        d = {k : od[k][0] for k in od}
        return d
    
        
    def _init_zmq(self, port=4747):
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.SUB)
        self._socket.connect("tcp://localhost:%s" % port)
        return
    
        
    def _draw_canvas(self):
        """
        A catch-all function that draws all the sub-widgets that make up the GUI
        """
        
        self._layout = QtGui.QGridLayout()
        self.setLayout(self._layout)

        self.show()
        self.resize(1000, 600)
        
        self._draw_plots()
        self._draw_text_inputs()
        self._draw_host_text()
        self._draw_apply_button()
        self._draw_launch_button()
        self._draw_status_text()
        
        return
    
        
    def _draw_plots(self):
        """
        Draws two plots on the left hand side of the widget, one monitoring
        damage levels and one doing hitrate plotting.
        """
        
        graphics_layout = pg.GraphicsLayoutWidget(border=(100,100,100))
        self._layout.addWidget(graphics_layout, 0, 0, 5, 1)

        self._upper_plot  = graphics_layout.addPlot(title="CSPAD Damage Level")
        self._upper_plot.addLegend()
        self._ds1_plot = self._upper_plot.plot(pen='g', name='DS1')
        self._ds2_plot = self._upper_plot.plot(pen='y', name='DS2')
        self._threshold_plot = self._upper_plot.plot(pen='r', name='Threshold')
        
        graphics_layout.nextRow()
        
        self._lower_plot   = graphics_layout.addPlot(title="Hit Rate")
        self._lower_plot.addLegend()
        self._xtal_plot    = self._lower_plot.plot(pen='g', name='xtal')
        self._diffuse_plot = self._lower_plot.plot(pen='y', name='diffuse')
        
        return
    
        
    def _draw_text_inputs(self):
        """
        Generates a GUI that can accept parameter values.
        """
        
        params = [
                  {'name': 'Saturation Threshold', 'type': 'int', 'value': 10000, 'suffix': 'ADU'},
                  {'name': 'Area Threshold', 'type': 'int', 'value': 5, 'suffix': '%', 'limits': (0, 100)},
                  {'name': 'Consecutive Shots', 'type': 'int', 'value': 5, 'limits': (1,120)}
                 ]

        self._params = Parameter.create(name='params', type='group', children=params)
        self._params.sigTreeStateChanged.connect(self._enable_apply)

        t = ParameterTree()
        t.setParameters(self._params, showTop=False)
        
        self._layout.addWidget(t, 0, 1)
        
        return
    
        
    def _draw_host_text(self):
        text = ['daq-cxi-dss07',
                'daq-cxi-dss08',
                'daq-cxi-dss09',
                'daq-cxi-dss10',
                'daq-cxi-dss11',
                'daq-cxi-dss12']
        text = ', '.join(text)
        self._host_text_widget = QtGui.QTextEdit(text)
        self._layout.addWidget(self._host_text_widget, 1, 1)
        return
        

    def _draw_status_text(self):
        text = 'No current running processes....' + '\n' * 10
        self._status_text_widget = QtGui.QLabel(text)
        self._layout.addWidget(self._status_text_widget, 2, 1)
        return
    
        
    def _draw_apply_button(self):
        self._apply_btn = QtGui.QPushButton('Apply Changes')
        self._layout.addWidget(self._apply_btn, 3, 1)
        self._apply_btn.clicked.connect(self.apply_parameters)
        self._apply_btn.setStyleSheet("background-color: grey")
        return
    
        
    def _draw_launch_button(self):
        self._launch_btn = QtGui.QPushButton('Launch')
        self._layout.addWidget(self._launch_btn, 4, 1)
        self._launch_btn.clicked.connect(self._launch_toggle)
        self._launch_btn.setStyleSheet("background-color: green")
        return
    
        
    def _enable_apply(self):
        """
        Indicate that new parameters are ready to be transmitted to a
        monitor process.
        """
        self._changes_ready_to_transmit = 1
        self._apply_btn.setStyleSheet("background-color: green")
        return
    
        
    def _launch_toggle(self):
        """
        If a monitor process is not running, start one. If one is running,
        shut it down.
        """
        
        if self.monitor_status == 'running':
            self.shutdown_monitor()
            self._launch_btn.setStyleSheet("background-color: red")
            
        elif self.monitor_status == 'ready':
            self.launch_monitor()
            self._launch_btn.setStyleSheet("background-color: red")
            
        else:
            raise RuntimeError('Status of remote monitor process is `%s`. '
                               'Process may have become disconnected from host.'
                               ' Recommended you check the host machines and '
                               'ensure lost processes are not running on them. '
                               'Manually kill those processes if '
                               'possible.' % self.monitor_status)
                               
        return
    
        
    def set_damage(self, ds1_data, ds2_data):
        self._ds1_plot.setData(ds1_data)
        self._ds2_plot.setData(ds2_data)
        return
    

    def set_hitrate(self, xtal_hitrate, diffuse_hitrate):
        self._xtal_plot.setData(xtal_hitrate)
        self._diffuse_plot.setData(diffuse_hitrate)
        return
    
        
    def set_status_text(self, text):
        self._status_text_widget.setText(text)
        return
    

    def launch_monitor(self):
        
        # retrieve the current values for each threshold from the text boxes
        p_dict = self.parameters
        area_threshold = int(32 * 185 * 388 * p_dict['Area Threshold']) # value in px
        
        # this is going to spawn an MPI process that will broadcast messages
        # using ZMQ
        guardain.run_mpi(p_dict['Saturation Threshold'],
                         p_dict['Consecutive Shots'],
                         area_threshold,
                         self.hosts)
        
        return


    def shutdown_monitor(self):
        return
    
        
    def apply_parameters(self):
        """
        Communicate new parameters to monitor process via ZMQ.
        """

        if self._changes_ready_to_transmit:
            # todo : do the transmit
            self._apply_btn.setStyleSheet("background-color: grey")
            self._changes_ready_to_transmit = 0

        return
        
        
    def update(self):
        """
        Update all data fields with accumulated statistics from the monitor
        process. 
        """
        
        # get data from the monitor process
        # we'll use a -1 to indicate something went wrong...
        mon_data = self._socket.recv()
        minus_one_array = np.ones(60*5) * -1
        
        self.set_damage(mon_data.get('ds1_damage', minus_one_array),
                        mon_data.get('ds2_damage', minus_one_array))
                        
        self.set_hitrate(mon_data.get('xtal_hitrate',    minus_one_array),
                         mon_data.get('diffuse_hitrate', minus_one_array))
                         
        num_procs        = mon_data.get('num_procs', -1)
        per_proc_rate    = mon_data.get('per_proc_rate', -1)
        evts_processed   = mon_data.get('evts_processed', -1)
        
        try:
            num_active_hosts = len(mon_data['hosts'])
        except:
            num_active_hosts = -1
        
        text = ['Monitor Statistics',
                '------------------',
                'Active processes      : %d'      % num_procs,
                'Active hosts          : %d'      % num_active_hosts,
                'Per-process data rate : %.2f Hz' % per_proc_rate,
                'Total data rate       : %.2f Hz' % per_proc_rate * num_procs,
                'Events processed      : %d'      % evts_processed,
                '1min xtal hitrate     : %.2f'    % np.mean(mon_data['xtal_hitrate'][:60])
                '1min diffuse hitrate  : %.2f'    % np.mean(mon_data['diffuse_hitrate'][:60])
                ]
        
        self.set_status_text('\n'.join(text))
        
        return
        
    
def main():
    
    app = QtGui.QApplication([])
    test_gui = TrapdoorWidget()
    print test_gui.hosts
    
    app.exec_()
    
    return
    

if __name__ == '__main__':
    main()
