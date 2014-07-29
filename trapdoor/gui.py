
"""
Contains the implementation of the GUI used to monitor and control trapdoor
"""

import sys
import zmq
import numpy as np
import socket

from pyqtgraph.Qt import QtGui, QtCore
import pyqtgraph as pg
from pyqtgraph.parametertree import Parameter, ParameterTree

#from trapdoor import guardian
import guardian


class TrapdoorWidget(QtGui.QWidget):
    """
    Provides methods both for launching trapdoor and looking at feedback from
    launched processes.
    """
    
    colors = ['r', 'g', 'y']
    
    def __init__(self, communication_freq=50):
        
        super(TrapdoorWidget, self).__init__()
        
        self.setWindowTitle('Trapdoor: CSPAD Monitoring')
        self._draw_canvas()

        self.timer = QtCore.QTimer()
        self.timer.timeout.connect(self.recv_data)
        self.timer.start(communication_freq)

        self._monitor_status = 'ready'
        self._init_zmq()
        
        return
        
        
    @property
    def threshold_names(self):
        # this is fixed for now, but I want to make it expandable later...
        return ['damage', 'xtal', 'diffuse']
    
        
    @property
    def monitor_status(self):
        if self._monitor_status in ['ready', 'running']:
            ms = self._monitor_status
        else:
            ms = 'unknown'
        return ms
        
        
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
        
        
    @property
    def monitor_list(self):
        """
        Returns
        -------
        monitor_list : list
            A list of tuples, where each entry corresponds to a threshold 
            monitor in the format (name, adu_threshold, area_threshold)
        """
        
        monitor_list = []
        
        p = self.parameters
        for name in self.threshold_names:
            adu  = p['%s :: Saturation Threshold' % name]
            area = p['%s :: Area Threshold' % name]
        
            monitor_list.append( (name, adu, area) )
        
        return monitor_list
    
        
    def _init_zmq(self, sub_port=4747, push_port=4748):
        
        self._zmq_context = zmq.Context()
        master_host = socket.gethostbyname_ex(self.hosts[0])[-1][0] # gives ip
        topic = 'stats'
        
        print 'GUI: binding %s:%d for ZMQ SUB' % (master_host, sub_port)
        self._zmq_sub = self._zmq_context.socket(zmq.SUB)
        self._zmq_sub.connect('tcp://%s:%d' % (master_host, sub_port))
        self._zmq_sub.setsockopt(zmq.SUBSCRIBE, topic)
       
        print 'GUI: binding %s:%d for ZMQ PUSH' % (master_host, push_port) 
        self._zmq_push = self._zmq_context.socket(zmq.PUSH)
        self._zmq_push.bind('tcp://%s:%d' % (master_host, push_port))
        
        return
    
        
    def _send_zmq_message(self, identifier, msg):
        """
        Sends the message `msg` to an active monitor using a ZMQ push. The idea
        is that `identifier` tells the remote process what to do with the data
        in `msg`.
        """
        print 'GUI: sending message, %s %s' % (identifier, msg)
        self._zmq_push.send_pyobj((identifier, msg))
        return
    
        
    def _draw_canvas(self):
        """
        A catch-all function that draws all the sub-widgets that make up the GUI
        """
        
        layout = QtGui.QGridLayout()
        
        self.resize(1000, 600)
        
        self._draw_plots(layout)
        self._draw_text_inputs(layout)
        self._draw_host_text(layout)
        self._draw_apply_button(layout)
        self._draw_launch_button(layout)
        self._draw_status_text(layout)
        
        self.setLayout(layout)
        
        return
    
        
    def _draw_plots(self, layout):
        """
        Draws two plots on the left hand side of the widget, one monitoring
        damage levels and one doing hitrate plotting.
        """
        
        graphics_layout = pg.GraphicsLayoutWidget(border=(100,100,100))
        
        self._upper_plot = graphics_layout.addPlot(title="DS1")
        self._upper_plot.addLegend()
        
        self._upper_curves = []
        for i,n in enumerate( self.threshold_names ):
            self._upper_curves.append( self._upper_plot.plot(name=n, pen=self.colors[i]) )
        
        graphics_layout.nextRow()
        
        self._lower_plot = graphics_layout.addPlot(title="DS2")
        self._lower_plot.addLegend()
        
        self._lower_curves = []
        for i,n in enumerate( self.threshold_names ):
            self._lower_curves.append( self._lower_plot.plot(name=n, pen=self.colors[i]) )
        
        layout.addWidget(graphics_layout, 0, 0, 5, 1)
        
        return
    
        
    def _draw_text_inputs(self, layout):
        """
        Generates a GUI that can accept parameter values.
        """
        
        params = [
                  {'name': 'damage :: Saturation Threshold', 'type': 'int', 'value': 50000, 'suffix': 'ADU'},
                  {'name': 'damage :: Area Threshold', 'type': 'int', 'value': 20, 'suffix': '%', 'limits': (0, 100)},
                  
                  {'name': 'xtal :: Saturation Threshold', 'type': 'int', 'value': 5000, 'suffix': 'ADU'},
                  {'name': 'xtal :: Area Threshold', 'type': 'int', 'value': 1, 'suffix': '%', 'limits': (0, 100)},
                  
                  {'name': 'diffuse :: Saturation Threshold', 'type': 'int', 'value': 1000, 'suffix': 'ADU'},
                  {'name': 'diffuse :: Area Threshold', 'type': 'int', 'value': 20, 'suffix': '%', 'limits': (0, 100)}
                 ]

        self._params = Parameter.create(name='params', type='group', children=params)
        self._params.sigTreeStateChanged.connect(self._enable_apply)

        t = ParameterTree()
        t.setParameters(self._params, showTop=False)
        
        layout.addWidget(t, 0, 1)
        
        return
    
        
    def _draw_host_text(self, layout):
        
        text = ['daq-cxi-dss07'] #,
                #'daq-cxi-dss08',
                #'daq-cxi-dss09',
                #'daq-cxi-dss10',
                #'daq-cxi-dss11',
                #'daq-cxi-dss12']
        
        self._host_text_widget = QtGui.QTextEdit(', \n'.join(text))
        
        layout.addWidget(self._host_text_widget, 1, 1)
        
        return
        

    def _draw_status_text(self, layout):
        
        text = 'No current running processes....' + '\n' * 10
        self._status_text_widget = QtGui.QLabel(text)
        
        layout.addWidget(self._status_text_widget, 2, 1)
        
        return
    
        
    def _draw_apply_button(self, layout):
        
        self._apply_btn = QtGui.QPushButton('Apply Changes')
        self._apply_btn.clicked.connect(self.apply_parameters)
        self._apply_btn.setStyleSheet("background-color: grey")
        
        layout.addWidget(self._apply_btn, 3, 1)
        
        return
    
        
    def _draw_launch_button(self, layout):
        
        self._launch_btn = QtGui.QPushButton('Launch')
        self._launch_btn.clicked.connect(self._launch_toggle)
        self._launch_btn.setStyleSheet("background-color: green")
        
        layout.addWidget(self._launch_btn, 4, 1)
        
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
            self._launch_btn.setStyleSheet("background-color: green")
            self._launch_btn.setText('Launch')
            self._monitor_status = 'ready'
            
        elif self.monitor_status == 'ready':
            self.launch_monitor()
            self._launch_btn.setStyleSheet("background-color: red")
            self._launch_btn.setText('Shutdown')
            self._monitor_status = 'running'
            
        else:
            raise RuntimeError('Status of remote monitor process is `%s`. '
                               'Process may have become disconnected from host.'
                               ' Recommended you check the host machines and '
                               'ensure lost processes are not running on them. '
                               'Manually kill those processes if '
                               'possible.' % self.monitor_status)
                               
        return
    
        
    def _set_plot_data(self, hitrates):
        
        for i,n in enumerate(self.threshold_names):
            print 'setting data for %d:%s | %f' % (i,n, hitrates[:,i,0].mean())
            self._lower_curves[i].setData( hitrates[:,i,0] )
            self._upper_curves[i].setData( hitrates[:,i,1] )
            
        return
    
        
    def set_status_text(self, text):
        self._status_text_widget.setText(text)
        return
    

    def launch_monitor(self):
        """
        Spawn MPI process that will broadcast messages using ZMQ
        """
        self.set_status_text('Launching monitor process...')
        print 'GUI: Launching monitor process...'
        guardian.run_mpi(self.monitor_list, self.hosts)
        self.set_status_text('Monitor launched, waiting for reply (takes ~30 sec).')
        print 'GUI: Monitor launched'
        return
    

    def shutdown_monitor(self):
        self._send_zmq_message('shutdown', None)
        return
    
        
    def apply_parameters(self):
        """
        Communicate new parameters to monitor process via ZMQ.
        """

        if self._changes_ready_to_transmit:
            self._send_zmq_message('set_parameters', self.parameters)
            self._apply_btn.setStyleSheet("background-color: grey")
            self._changes_ready_to_transmit = 0

        return
    
        
    def recv_data(self):
        """
        Update all data fields with accumulated statistics from the monitor
        process. 
        """
        
        # get data from the monitor process
        try:
            topic    = self._zmq_sub.recv(zmq.NOBLOCK)
            mon_data = self._zmq_sub.recv_pyobj(zmq.NOBLOCK)
            print 'GUI: recieved stats msg'
        except zmq.Again as e:
            #print 'GUI: No remote detected...'
            return

        # if we got data, but are in the 'ready' state, toggle to 'running' state
        if (mon_data != None) and (self.monitor_status == 'ready'):
            self._launch_btn.setStyleSheet("background-color: red")
            self._launch_btn.setText('Shutdown')
            self._monitor_status == 'running'
        
        # --> ensure names of threshold monitors match
        num_monitors = mon_data.get('num_monitors', -1)
        
        if not mon_data['monitor_names'] == self.threshold_names:
            print 'Threshold monitor mismatch between GUI and remote Guardian'
            print 'GUI:    %s' % str(self.threshold_names)
            print 'Remote: %d' % str(mon_data['monitor_names'])
            raise RuntimeError('No known method exists to resolve mismatch conflict') # todo
        
        # --> ensure that the set parameters match 
        local_parms = self.parameters
        for i,n in enumerate( mon_data['monitor_names'] ):
            remote_adu  = mon_data['adu_thresholds'][i]
            remote_area = mon_data['area_thresholds'][i]
            
            if not remote_adu == local_parms['%s :: Saturation Threshold' % n]:
                print 'ADU Threshold mismatch between GUI and remote (%d/%d)' % remote_adu, local_parms['%s :: Saturation Threshold' % n]
                raise RuntimeError('No known method exists to resolve mismatch conflict') # todo
                
            if not remote_area == local_parms['%s :: Area Threshold' % n]:
                print 'AREA Threshold mismatch between GUI and remote (%d/%d)' % (remote_area, local_parms['%s :: Area Threshold' % n])
                raise RuntimeError('No known method exists to resolve mismatch conflict') # todo
        
            
        # --> get metadata from the CxiGuardian cls
        num_cameras  = mon_data.get('num_cameras', -1)
        history_size = mon_data.get('history_size', -1) # not reported ATM
        window_size  = mon_data.get('window_size', -1)  # not reported ATM
        shutter_tshd = mon_data.get('shutter_control_threshold', -1)
        

        # --> get data about running processes from the MapReduce cls 
        num_procs        = mon_data.get('num_procs', -1)
        per_proc_rate    = mon_data.get('per_proc_rate', -1)
        evts_processed   = mon_data.get('evts_processed', -1)
        try:
            num_active_hosts = len(mon_data['hosts'])
        except:
            num_active_hosts = -1
        
            
        # --> get damage/hitrate data
        hitrates = mon_data['hitrates']

        # compute hitrates over the last minute
        one_min_hitrate = {}
        one_min_index = int(60.0 * window_size / 120.0)
        for i,n in enumerate( mon_data['monitor_names'] ):
            one_min_hitrate[n] = np.mean( hitrates[i,:one_min_index] )
            
            
        # --- report to the user ---
        
        # update plots
        self._set_plot_data(hitrates)
        
        
        # print to console
        # todo: consider printing set threshold parameters
        text = ['Monitor Statistics',
                '---------------------------------',
                'Active processes      : %d'      % num_procs,
                'Active hosts          : %d'      % num_active_hosts,
                'Per-process data rate : %.2f Hz' % per_proc_rate,
                'Total data rate       : %.2f Hz' % (per_proc_rate * num_procs,),
                'Events processed      : %d'      % evts_processed,
                'Active cameras        : %d'      % num_cameras,
                'Shutter threshold     : %d ADU'  % shutter_tshd,
                '---------------------------------',
                '1min CSPAD damage lvl : %.2f'    % one_min_hitrate['damage'],
                '1min xtal hitrate     : %.2f'    % one_min_hitrate['xtal'],
                '1min diffuse hitrate  : %.2f'    % one_min_hitrate['diffuse'],
                '---------------------------------'
                ]
        
        self.set_status_text('\n'.join(text))
        
        return
        
    
def main():
    
    app = QtGui.QApplication(sys.argv)
    
    test_gui = TrapdoorWidget()
    test_gui._set_plot_data( np.zeros((120,3,2)) )
    test_gui.show()
    
    print test_gui.hosts
    print test_gui.monitor_list
    
    sys.exit(app.exec_())
    
    return
    

if __name__ == '__main__':
    main()
