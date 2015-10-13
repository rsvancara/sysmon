#!/usr/bin/env python

#
#
#  
#
#

import pika
import logging
import json

class Comm:

    def __init__(self):
        self.connection = None
        self.channel = None
        self.host = '10.10.0.134'
        self.credentials = pika.PlainCredentials('logs', 'logs')
        self.port = 5672
        self.virtual_host = '/'
        self.l = logging.getLogger('sysmon') 

    def ampq_connect(self):
        self.connection = None
        self.channel = None
        if self.connection is not None:
            pass

        while self.connection is None:
            self.l.info("Attempting ampq connection")

            try:
                self.credentials = pika.PlainCredentials('logs', 'logs')
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.10.0.134',port=5672,virtual_host='/',credentials=self.credentials,retry_delay=5))
                self.channel = self.connection.channel()
                return
            except Exception as e:
                self.l.error("Could not connect to rabbitmq %s" % (e))
            # Wait 10 seconds for the next connectiona attempt
            time.sleep(10)

    def send_message(self,stats):
        if self.channel is not None:
            try:
                self.channel.basic_publish(exchange='logs',routing_key='',body=json.dumps(stats))
            except Exception as e:

                self.l.error("Error publishing to ampq exchange, perhaps rabbitmq is dead?? %s" % (e))
                self.channel = None
                self.connection = None
                self.ampq_connect()
        else:
            self.ampq_connect()
  


def main():
    pass


if __name__ == "main": 
    pass

