#!/usr/bin/env python

import pika
import json
import pprint
import timeit


credentials = pika.PlainCredentials('logs', 'logs')
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost',5672,'/',credentials))
#connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

#channel.exchange_declare(exchange='logs',type='fanout')

#result = channel.queue_declare(exclusive=True)
#queue_name = result.method.queue
channel.queue_bind(exchange='logs',queue='logs')
#channel.queue_bind(exchange='logs',
#                  queue=queue_name)

print ' [*] Waiting for logs. To exit press CTRL+C'

def callback(ch, method, properties, body):
    
    start_time = timeit.default_timer()
    #print " [x] %r" % (body,)
    stats = json.loads(body)
    
    elapsed = timeit.default_timer() - start_time
    print ("%s - %s" % (stats['uuid'],elapsed)) 
    
    
    #print stats
    #pp = pprint.PrettyPrinter(indent=4)
    #pp.pprint(stats)
    
    

channel.basic_consume(callback,queue='logs',no_ack=True)

channel.start_consuming()