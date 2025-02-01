import pika
import psutil
import time
import subprocess
import os
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
load_dotenv()








HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
QUEUE = os.getenv("QUEUE")
VHOST = os.getenv("VHOST")
API_STATS_PORT = os.getenv("API_STATS_PORT")


QUEUE_THRESHOLD_MAX = int(os.getenv("QUEUE_THRESHOLD_MAX"))
QUEUE_THRESHOLD_MIN = int(os.getenv("QUEUE_THRESHOLD_MIN"))
CPU_THRESHOLD = int(os.getenv("CPU_THRESHOLD"))
MAX_WORKERS   = int(os.getenv("MAX_WORKERS"))
MIN_WORKERS = int(os.getenv("MIN_WORKERS") )
MONITORING_INTERVAL = int(os.getenv("MONITORING_INTERVAL"))
CONSUMER = os.getenv("CONSUMER")



URL = f"http://{HOST}:{API_STATS_PORT}/api/queues/{VHOST}/{QUEUE}"
worker_processes = [] 

connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
channel = connection.channel()
channel.queue_declare(queue=QUEUE, arguments={
    'x-max-length': 1000
})

def get_queue_length():
    try:
        response = requests.get(URL, auth=HTTPBasicAuth(USER, PASSWORD))
        response.raise_for_status()
        data = response.json()
        queue_length = data.get("messages", 0)
        return queue_length
    except requests.exceptions.RequestException as e:
        print(f"Error al conectar con RabbitMQ API: {e}")
        return -1



def start_worker():
    global worker_processes
    process = subprocess.Popen(['python3', f'{CONSUMER}.py'])
    worker_processes.append(process)

def stop_worker():
    global worker_processes
    process = worker_processes.pop()
    process.terminate()
    process.wait()


for _ in range(MIN_WORKERS):
    start_worker()

while True:
    queue_length = get_queue_length()
    cpu_percentage = psutil.cpu_percent(interval=0)

    print(f"CPU % {cpu_percentage} | Longitud de la cola: {queue_length} mensajes | Workers activos: {len(worker_processes)}")

    if (queue_length > QUEUE_THRESHOLD_MAX or cpu_percentage > CPU_THRESHOLD) and len(worker_processes) < MAX_WORKERS:
        start_worker()

    elif (queue_length < QUEUE_THRESHOLD_MIN and cpu_percentage < CPU_THRESHOLD) and len(worker_processes) > MIN_WORKERS:
        stop_worker()

    time.sleep(MONITORING_INTERVAL)
