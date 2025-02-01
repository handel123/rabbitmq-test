
import pika
import time

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost')
)
channel = connection.channel()

channel.queue_declare(queue='cola_auto', arguments={
    'x-max-length': 1000
})


###


def callback(ch, method, properties, body):
    try:
        time.sleep(2)  #simulamos una tarea que toma tiempo
        #print(f"Mensaje recibido: {body}")
        

        # a este punto ya procesamos el mensaje, por lo que la próxima linea mandará el mensaje de confirmación (ACK)
        # a rabbitmq para decirle que fue procesado con exito.
        ch.basic_ack(delivery_tag=method.delivery_tag) 

    except Exception as e:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True) ## aqui rechazo el mensaje y lo vuelvo a enviar a la cola

channel.basic_qos(prefetch_count=1) #cantidad de mensajes que se pueden procesar a la vez
channel.basic_consume(queue='cola_auto', on_message_callback=callback, auto_ack=False)

print("Worker ejecutado")
channel.start_consuming()
