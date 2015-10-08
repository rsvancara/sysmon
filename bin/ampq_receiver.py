#!/usr/bin/env python

import pika
import json
import pprint

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='logs',type='fanout')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='logs',
                   queue=queue_name)

print ' [*] Waiting for logs. To exit press CTRL+C'

def callback(ch, method, properties, body):
    #print " [x] %r" % (body,)
    stats = json.loads(body)
    #print stats
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(stats)

channel.basic_consume(callback,queue=queue_name,no_ack=True)

channel.start_consuming()