#!/usr/bin/env python

import sys, time
import os
import re
from sysmon.daemon import Daemon
import json
import socket
import logging
import pika
import uuid
import datetime
import base64


# Inherit from parent, override run method
class SysmonDaemon(Daemon):
    

    #   #self.__init__(self,'/var/run/sysmon.pid')
    #    super(Daemon, self).__init__('/var/run/sysmond.pid')
    def run(self):
        
        self.l = logging.getLogger('sysmon')
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.l.addHandler(ch)
        
        self.initialize()
        
    
    def initialize(self):
        # Setup our rabbitmq stuff
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='logs',type='fanout')
        self.channel.basic_publish(exchange='logs',routing_key='',body='{"client":' + socket.gethostname() + '}')
        
        # Compile our regex for the minimal performance gain
        self.dev_re = re.compile('([A-Za-z0-9\.]+):\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)')
        self.meminfo_re = re.compile('([\w]+):\s(\d+)\s')
        self.proc_stats_re = re.compile('([A-Za-z0-9]+)\s+(\d+)')
        self.diskstatus_re = re.compile('\s+(\d+)\s+(\d+)\s(\w+)\s(\d+)\s(\d+)\s(\d+)\s(\d+)\s(\d+)\s(\d+)\s(\d+)\s(\d+)\s(\d+)\s(\d+)\s(\d+)')
        while True:
            self.getProcesses()
            time.sleep(2)        
    
    def getUUID(self ):
        return base64.urlsafe_b64encode(uuid.uuid4().bytes).replace('=','')
    
        
    # Get all the processes that are not system
    # processes
    def getProcesses(self, ):
        """ Get all system processes """
        # walk the /proc
        stats = {}
        stats['uuid'] = self.getUUID()
        stats['hostname'] = socket.gethostname()
        d = datetime.datetime.utcnow()
        stats['timestamp_year'] = d.strftime('%y')
        stats['timestamp_month'] = d.strftime('%m')
        stats['timestamp_day'] = d.strftime('%d')
        stats['timestamp_hour'] = d.strftime('%H')
        stats['timestamp_minute'] = d.strftime('%M')
        stats['timestamp_second'] = d.strftime('%S')
        
        for item in os.listdir('/proc'):
            if re.match('[\d]+',item):
                # Start collecting statistics
                # We only want statistics for processes that
                # are not system processes, i.e. user jobs
                stat_info = None
                try:
                    stat_info = os.stat('/proc/' + item + '/status')
                except Exception, err:
                    self.l.error(err)
                
                if stat_info is not None:   
                    # It is assumed that the ownership of the pid directory
                    # represents the owner of the process and as such we can
                    # use this to determine if this is a "non-system" or "non-root"
                    # process
                    if stat_info.st_uid >= 500:
                        self.getProcStatistics(item,stats)
        
        # Get memory information           
        self.getMemInfo(stats)
        
        # Get network statistics
        self.getNetworkStats(stats)
        
        # Get processor statistics
        self.getProcessorStats(stats)
        
        # Get Disk statistics
        self.getDiskStats(stats)
        
        # Get Infiniband statistics
        self.getInfinibandStats(stats)
        
        self.channel.basic_publish(exchange='logs',routing_key='',body=json.dumps(stats))
        #print stats
                    
    def getMemInfo(self, stats):
        if os.path.exists('/proc/meminfo'):
            with open('/proc/meminfo') as f:
                for line in f.readlines():
                    m = self.meminfo_re.match(line.rstrip())
                    if m is not None:
                        stats[m.group(1)] = m.group(2)               
                    m = None
                f.close()
            
    def getNetworkStats(self,stats):
        if os.path.exists('/proc/net/dev'):
            with open('/proc/net/dev') as f:
                for line in f.readlines():
                    m = self.dev_re.match(line)
                    if m is not None:
                        stats['net'] = {}
                        stats['net'][m.group(1)] = {}
                        stats['net'][m.group(1)]['rx_bytes'] = m.group(2)
                        stats['net'][m.group(1)]['rx_packets'] = m.group(3)
                        stats['net'][m.group(1)]['rx_errors'] = m.group(4)
                        stats['net'][m.group(1)]['rx_drop'] = m.group(5)
                        stats['net'][m.group(1)]['rx_fifo'] = m.group(6)
                        stats['net'][m.group(1)]['rx_frame'] = m.group(7)
                        stats['net'][m.group(1)]['rx_compressed'] = m.group(8)
                        stats['net'][m.group(1)]['multicast'] = m.group(9)
                        stats['net'][m.group(1)]['tx_bytes'] = m.group(10)
                        stats['net'][m.group(1)]['tx_packets'] = m.group(10)
                        stats['net'][m.group(1)]['tx_errs'] = m.group(12)
                        stats['net'][m.group(1)]['tx_fifo'] = m.group(13)
                        stats['net'][m.group(1)]['tx_colls'] = m.group(14)
                        stats['net'][m.group(1)]['tx_carrier'] = m.group(15)
                        stats['net'][m.group(1)]['tx_compressed'] = m.group(16)
                        
                    m = None
    
    def getProcessorStats(self,stats ):
        # The cpu columns in /proc/stat show the amount of time spent doing
        # each type, measured in units of USER_HZ (clock ticks).
        # sysconf(3) describes SC_CLK_TCK as the number of clock ticks per second.
        # Ie. this calculated how many clock ticks have elapsed between each
        # reading of /proc/stat
        stats['cpu'] = {}
        stats['cpu']['sc_clk_tck'] = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
        if os.path.exists('/proc/stats'):
            with open('/proc/diskstatus') as f:
                for line in f.readlines():
                    if re.match('cpu',line):
                        m = self.proc_stats_re.match(line)
                        if m is not None:
                            stats['cpu'][m.group(1)] = {}
                            stats['cpu'][m.group(1)]['user'] = m.group(2)
                            stats['cpu'][m.group(1)]['nice'] = m.group(3)
                            stats['cpu'][m.group(1)]['system'] = m.group(4)
                            stats['cpu'][m.group(1)]['idle'] = m.group(5)
                            stats['cpu'][m.group(1)]['iowait'] = m.group(6)
                            stats['cpu'][m.group(1)]['irq'] = m.group(7)
                            stats['cpu'][m.group(1)]['softirq'] = m.group(8)
                            stats['cpu'][m.group(1)]['steal'] = m.group(9)
                            stats['cpu'][m.group(1)]['guest'] = m.group(10)
                            stats['cpu'][m.group(1)]['guest_nice'] = m.group(11)
                        

    def getDiskStats(self,stats ):
        if os.path.exists('/proc/diskstatus'):
            with open('/proc/diskstatus') as f:
                for line in f.readlines():
                    if re.match('sda|hda',line):
                        m = self.diskstatus_re.match(line)
                        stats['device'][m.group(3)]['reads_completed'] = m.group(4) # of reads completed
                        stats['device'][m.group(3)]['reads_merged'] = m.group(5) # of reads merged
                        stats['device'][m.group(3)]['sectors_read'] = m.group(6)  # of sectors read
                        stats['device'][m.group(3)]['time_spent_reading'] = m.group(7) # of milliseconds spent reading
                        stats['device'][m.group(3)]['writes_completed'] = m.group(8) # of writes completed
                        stats['device'][m.group(3)]['writes_merged'] = m.group(9) # of writes merged
                        stats['device'][m.group(3)]['sectors_written'] = m.group(10) # of sectors written
                        stats['device'][m.group(3)]['time_spent_writing'] = m.group(11) # of milliseconds spent writing
                        stats['device'][m.group(3)]['ios_in_progress'] = m.group(12) # of I/Os currently in progress
                        stats['device'][m.group(3)]['time_spent_ios'] = m.group(13)  # of milliseconds spent doing I/Os
                        stats['device'][m.group(3)]['weighted_time_ios'] =  m.group(14) #weighted # of milliseconds spent doing I/Os
                f.close()
    
    def getInfinibandStats(self,stats):
        if os.path.exists('/sys/class/infiniband'):
            items = os.listdirs('/sys/class/infiniband')

    def getProcStatistics(self,item,stats):
        
        path = '/proc/' + item
        stats[item] = {}
        #
        # Read status /proc/status
        #
        f = None
        try:
            if os.path.exists(path + '/status'):
                with open(path + '/status') as f:
                    for line in f.readlines():
                        m = re.match('([\w]+):\s(\d+)',line.rstrip())
                        if m is not None:
                            #print("%s %s" % (m.group(1), m.group(2)))
                            stats[item][m.group(1)] = m.group(2)
                        m = None
                    f.close()
        except Exception, err:
            self.l.error(err)
        finally:
            if f is not None:
                f.close()

        #
        # /proc/[pid]/cmdline - Holds complete command line for the process unless it is a
        # zombie, otherwise the file is empty
        #
        try:
            if os.path.exists(path + '/cmdline'):
                with open(path + '/cmdline') as f:
                    #print f.readline()
                    stats[item]['cmdline'] = f.readline()
                    f.close()
        except Exception, err:
            self.l.error(err)
        finally:
            if f is not None:
                f.close()                   
        #
        # /proc/[pid]/cwd - Symbolic link to the current working directory
        #
        try:
            if os.path.exists(path + '/cwd'):
                stats[item]['cwd'] = os.readlink(path + '/cwd')
                
        except Exception, err:
            self.l.error(err)
        finally:
            if f is not None:
                f.close()
        
        #
        # /proc/[pid]/fd/  - This is the subdirectory containing one entry for
        # for each file which the process has open, named by its file descripter and symbolic
        # linked to the actual file !! some of these are sockets or pipes
        #
        try:
            if os.path.exists(path + '/fd'):
                stats[item]['fd'] = []
                for fnode in os.listdir(path + '/fd'):
                    stats[item]['fd'].append(os.readlink(path + '/fd/' + fnode))
        except Exception, err:
            self.l.error(err)
        finally:
            if f is not None:
                f.close()
        #
        # /proc/[pid]/io - contains the I/O statistics for the process 
        #
        try:
            if os.path.exists(path + '/io'):
                with open(path + '/io') as f:
                    for line in f.readlines():
                        m = re.match('([\w]+):\s(\d+)',line.rstrip())
                        if m is not None:
                             stats[item][m.group(1)] = m.group(2)
                        m = None
                    f.close()
        except Exception, err:
            self.l.error(err)
        finally:
            if f is not None:
                f.close()
        
    def test(self, ):
        
        self.l = logging.getLogger('sysmon')
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.l.addHandler(ch)
        
        self.initialize()

            
    

if __name__ == '__main__':
    
    sysmond = SysmonDaemon('/tmp/daemon-example.pid')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            sysmond.start()
        elif 'stop' == sys.argv[1]:
            sysmond.stop()
        elif 'restart' == sys.argv[1]:
            sysmond.restart()
        elif 'test' == sys.argv[1]:
            sysmond.test()
        else:
            print "Unknown command"
            sys.exit(2)
        sys.exit(0)
    else:
        print "usage: %s start|stop|restart" % sys.argv[0]
        sys.exit(2)    
    #parser = argparse.ArgumentParser()
    #parser.add_argument('--config', help='Configuration File')
    #args = parser.parse_args()
    
    #print arg.config
    
    #sys = sysmondaemon()
    
    