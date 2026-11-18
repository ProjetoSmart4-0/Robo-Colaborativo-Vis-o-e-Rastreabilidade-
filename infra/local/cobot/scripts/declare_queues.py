import os
import pika

url = os.getenv('RABBITMQ_URL')
params = pika.URLParameters(url) if url else pika.ConnectionParameters('localhost')
conn = pika.BlockingConnection(params)
ch = conn.channel()

ch.exchange_declare(exchange='cobot.dlx', exchange_type='direct', durable=True)
ch.queue_declare(queue='cobot_events_dlq', durable=True)
ch.queue_bind(queue='cobot_events_dlq', exchange='cobot.dlx', routing_key='cobot_events_dlq')

ch.queue_declare(
    queue='cobot_events',
    durable=True,
    arguments={
        'x-dead-letter-exchange': 'cobot.dlx',
        'x-dead-letter-routing-key': 'cobot_events_dlq',
    },
)

ch.queue_declare(queue='cobot_tasks', durable=True)
ch.queue_declare(queue='ur3e_tasks', durable=True)

conn.close()
print('filas declaradas: cobot_events, cobot_events_dlq, cobot_tasks e ur3e_tasks')