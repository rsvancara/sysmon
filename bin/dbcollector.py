#!/usr/bin/env python

#
# Prototype DB Collector code that
# listens to  a connection to collect data
#
import sys, time
import os
from sysmon.daemon import Daemon # Creates the standard two fork daemon
from sysmon.Comm import Comm  # Communication Library for RabbitMQ
import threading
from Queue import Queue
import logging
import pika
from pymongo import MongoClient
from pymongo import Connection
import json

# Create a queue with the size of 100
s_queue = Queue(100)

# Connects to RabbitMQ and consumes the log information
class ConsumerThread(threading.Thread):
    def __init__(self, host, *args, **kwargs):
        super(ConsumerThread, self).__init__(*args, **kwargs)
        self.l = logging.getLogger('collector')
        self._host = host

    def callback(self, channel, method, properties, body):
        #print("{} received '{}'".format(self.name, body))
        # Load the event data into a queue
        if not s_queue.full():
            #print "adding to queue"
            s_queue.put(body)
    
            


    def run(self):
        credentials = pika.PlainCredentials("logs", "logs")

        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._host,credentials=credentials))

        channel = connection.channel()

        channel.queue_bind(exchange='logs',queue='logs')

        channel.basic_consume(self.callback,
                              "logs",
                              no_ack=True)
        
        channel.start_consuming()
        
# Writer queue responsible for emptying the queue 
class DbWriterThread(threading.Thread):
    
    def __init__(self, threadid, *args, **kwargs):
        super(DbWriterThread, self).__init__(*args, **kwargs)
        self.l = logging.getLogger('collector')
        print "initialized"
        
        self.connection = MongoClient('localhost')
        self.db = self.connection.logs.nodelogs
        
        
        
        self.item_array = []

    def run(self):
        while True:
            print "Running"
            if not s_queue.empty():
                stat = s_queue.get()
                print s_queue.qsize()
                self.item_array.append(stat)
                #print stat
            else:
                print s_queue.qsize()
                time.sleep(1)
                
            # Flush the records to the database
            if len(self.item_array) >= 20:
                
                for s in self.item_array:
                    stat = json.loads(s)
                    
                    self.db.insert(stat)

                self.item_array = []
                
    # def insertData(self,i,queue):
    #     while True:
    #         if not self.queue.empty():
    #             stat = self.queue.get()
    #             print stat
    #         else:
    #             time.sleep(2)

    


class DbCollectorDaemon(Daemon):

    

    def run(self, ):
        self.l = logging.getLogger('collector')
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.l.addHandler(ch)
    

    
    
        self.intialize()
        
    def intialize(self,):

        # Worker threads
        for i in range(4):
            #worker = threading.Thread())
            worker = DbWriterThread(i)
            worker.start()
            
        # Consumer thread
        cs = ConsumerThread("localhost")
        cs.start()
        
        
        
        #self.rabbitcom = Comm()
        #self.connect = self.rabbitcom.ampq_connect()        
        
        
        
    def test(self, ):
        # Turn this on for verbose debugging of PIKA
        #logging.basicConfig(level=logging.INFO)
        #logging.getLogger('pika').setLevel(logging.DEBUG)

        self.l = logging.getLogger('collector')
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.l.addHandler(ch)
        
        self.intialize()
    


if __name__ == '__main__':

    collector = DbCollectorDaemon('/tmp/collector.pid')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            collector.start()
        elif 'stop' == sys.argv[1]:
            collector.stop()
        elif 'restart' == sys.argv[1]:
            collector.restart()
        elif 'test' == sys.argv[1]:
            collector.test()
        else:
            print "Unknown command"
            sys.exit(2)
        sys.exit(0)
    else:
        print "usage: %s start|stop|restart" % sys.argv[0]
        sys.exit(2)    

    
