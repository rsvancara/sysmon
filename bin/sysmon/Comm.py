#!/usr/bin/env python

#
#
#  RabbitMQ Communication Library makes
#  it easier to use rabbitMQ
#
#

import pika
import logging
import json
import time

class Comm:

    def __init__(self,host,username,password,virtualhost,exchange,queue,logger):
        self.connection = None
        self.channel = None
        self.host = host
        self.username = username
        self.password = password
        self.virtualhost = virtualhost
        self.exchange = exchange
        self.credentials = pika.PlainCredentials('logs', 'logs')
        self.port = 5672

        self.l = logging.getLogger(logger) 

    def ampq_connect(self):
        self.connection = None
        self.channel = None
        if self.connection is not None:
            self.l.debug("Connection is good")
            pass

        while self.connection is None:
            self.l.info("Attempting ampq connection")

            try:

                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host,
                                                                                    port=self.port,
                                                                                    virtual_host=self.virtualhost,
                                                                                    credentials=self.credentials))
                self.channel = self.connection.channel()
                return self.channel
            except Exception as e:
                self.l.error("Could not connect to rabbitmq %s" % (e))
            # Wait 10 seconds for the next connectiona attempt
            self.l.info("Waiting for connection")
            time.sleep(10)

    def send_message(self,stats):
        if self.channel is not None:
            try:
                self.channel.basic_publish(exchange=self.exchange,routing_key='',body=json.dumps(stats))
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

