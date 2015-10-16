#!/usr/bin/env python


#
# Monitors Torque and Maui for job information
#
import sys, time
import os
import re
from sysmon.daemon import Daemon # Creates the standard two fork daemon
from sysmon.Comm import Comm  # Communication Library for RabbitMQ
import socket
import logging
import uuid
import datetime
import base64
from subprocess import Popen, PIPE
import json
import xmltodict

# Inherit from parent, override run method
class JobmonDaemon(Daemon):


    def run(self):
        
        self.l = logging.getLogger('jobmon')
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.l.addHandler(ch)
        
        self.initialize()


    def initialize(self):

        self.rabbitcom = Comm('10.10.0.134','logs','logs','/','jobs','','jobmon')

        # Server main loop
        while True:
            self.getJobs()
            time.sleep(4)   

    def test(self, ):
        # Turn this on for verbose debugging of PIKA
        #self.l = logging.basicConfig(level=logging.ERROR)
        #logging.getLogger('pika').setLevel(logging.DEBUG)

        self.l = logging.getLogger('jobmon')
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.l.addHandler(ch)
        
        self.initialize()

    def getJobs(self,):
        print "working" 
        
        proc = Popen('qstat -f -x', shell=True, stdout=PIPE, stderr=PIPE)
        result = proc.communicate()
        if len(result) >= 1:

            qstatout = result[0] 
            jobs = [] 

            o = xmltodict.parse(qstatout)
            #Grab the top level data
            for (akey,avalue) in o.iteritems():
                # Next level is jobs
                for (nkey,nvalue) in avalue.iteritems():
                   #uuid = self.getUUID()                
                   jobs.append(nvalue )
                         
                   #for p in nvalue:
                   #        for (bkey,bvalue) in p.iteritems():  
                   #            #print ("%s\t%s" % (bkey,bvalue))
            #jsonstring = json.dumps(o)
            #print jsonstring

            # Parse the output into 
            #for line in qstatout.split('\n'):
            #    newjob = False
            #    jobid = ''
            #     
            #    m = re.match('^Job\sId:\s([A-Za-z0-9\.]+)',line)
            # 
            #    if m is not None:
            #        jobid = m.group(1)
            #        curjob = {}
            #        newjob = 1
            #        print jobid
            jsonstring = json.dumps(jobs)
            print jsonstring
	
    def S(self,key ):
        return key.replace('.','_')
    
    
    def getUUID(self):
        return re.sub('_|-|=','0',base64.urlsafe_b64encode(uuid.uuid4().bytes))

if __name__ == '__main__':
    jobmond = JobmonDaemon('/tmp/jobmon.pid')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            jobmond.start()
        elif 'stop' == sys.argv[1]:
            jobmond.stop()
        elif 'restart' == sys.argv[1]:
            jobmond.restart()
        elif 'test' == sys.argv[1]:
            jobmond.test()
        else:
            print "Unknown command"
            sys.exit(2)
        sys.exit(0)
    else:
        print "usage: %s start|stop|restart" % sys.argv[0]
        sys.exit(2)     
