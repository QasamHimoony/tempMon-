import pika
import subprocess
import logging
from influxdb import InfluxDBClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def callback(ch, method, properties, body):
    try:
        switch_id = body.decode('utf-8')
        temperature = get_temperature(switch_id)
        
        if temperature is not None:
            write_to_influxdb(switch_id, temperature)
        else:
            print(f"Failed to retrieve temperature for switch ID: {switch_id}")
    
    except Exception as e:
        print(f"Error processing message: {e}")


def get_temperature(switch_id):
    # Replace 'public' with your SNMP community string
     command = f"snmpget -v 2c -c public 192.168.1.1 1.3.6.1.4.1.9.9.13.1.3.1.3"
     result = subprocess.run(command, shell=True, capture_output=True, text=True)
    # Extract temperature from the result
     temperature = result.stdout.strip().split()[-1]
     return temperature


def write_to_influxdb(switch_id, temperature):
    try:
        client = InfluxDBClient(host='localhost', port=8086, username='admin', password='password', database='tempmon')
        data = [
            {
                "measurement": "temperature",
                "tags": {
                    "switch_id": switch_id
                },
                "fields": {
                    "value": float(temperature)
                }
            }
        ]
        client.write_points(data)
        logger.info(f"Temperature data written to InfluxDB for switch: {switch_id}")
    except Exception as e:
        logger.error(f"Error writing to InfluxDB for switch ID {switch_id}: {e}")


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='task_queue', durable=False)
channel.basic_consume(queue='task_queue', on_message_callback=callback, auto_ack=True)

print('Collector is waiting for messages. To exit press CTRL+C')
channel.start_consuming()