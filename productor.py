import pika
import time
MESSAGES_PER_SECOND = 3




connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='cola_auto', arguments={
    'x-max-length': 1000 
})

channel.basic_qos(prefetch_count=1)
message = "Test Message"

print(f"Enviando mensajes a una tasa de {MESSAGES_PER_SECOND} mensajes/segundo...")

try:
    while True:
        start_time = time.time()

        for _ in range(MESSAGES_PER_SECOND):
            channel.basic_publish(
                exchange='',
                routing_key='cola_auto',
                body=message
            )
        
        elapsed_time = time.time() - start_time
        sleep_time = max(0, 1 - elapsed_time) 
        time.sleep(sleep_time)

except KeyboardInterrupt:
    print("Deteniendo el productor...")
   

connection.close()