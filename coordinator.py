import pika
import psycopg2
from psycopg2 import Error
from influxdb import InfluxDBClient
import subprocess  # Import the subprocess module
import logging


def fetch_switches():
    try:
        # Connect to PostgreSQL
        with psycopg2.connect(
            dbname="postgres",    # Database name
            user="postgres",      # PostgreSQL username
            password="password",  # PostgreSQL password
            host="localhost"      # Host
        ) as conn:
            with conn.cursor() as cursor:
                # Fetch switch data from the switches table
                cursor.execute("SELECT id, name, ip, status FROM switches LIMIT 5")  # Limit to 5 switches
                switches = cursor.fetchall()
                
        return switches
    except Error as e:
        print(f"Error fetching switches: {e}")
        return []


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def callback(ch, method, properties, body):
    try:
        switch_id = body.decode('utf-8')
        temperature = get_temperature(switch_id)
        
        if temperature is not None:
            write_to_influxdb(switch_id, temperature)
            logger.info(f"Successfully wrote temperature for switch ID: {switch_id} to InfluxDB")
        else:
            logger.warning(f"Failed to retrieve temperature for switch ID: {switch_id}")
    
    except Exception as e:
        logger.error(f"Error processing message: {e}")


def get_temperature(switch_id):
    command = f"snmpget -v 2c -c public 192.168.1.100 1.3.6.1.4.1.9.9.13.1.3.1.3"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    # Extract temperature from the result
    temperature = result.stdout.strip().split()[-1]
    return temperature





# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def write_to_influxdb(switch_id, temperature):
    try:
        client = InfluxDBClient(host='localhost', port=8086, username='admin', password='password', database='temperature_db')
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
        logger.error(f"Error writing to InfluxDB: {e}")

def fetch_switches():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname="postgres",    # Database name
            user="postgres",      # PostgreSQL username
            password="password",  # PostgreSQL password
            host="localhost"      # Host
        )
        with conn.cursor() as cursor:
            # Fetch switch data from the switches table
            cursor.execute("SELECT id, name, ip, status FROM switches LIMIT 5")  # Limit to 5 switches
            switches = cursor.fetchall()
        return switches
    except psycopg2.Error as e:
        logger.error(f"Error fetching switches: {e}")
        return []

# Fetch switch data from PostgreSQL
switches = fetch_switches()
if not switches:
    logger.info("No switches found in the database.")
    exit()

# Connect to RabbitMQ
try:
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='task_queue', durable=False)

    # Send switch IDs to the task queue
    for switch in switches:
        switch_ip = switch[2]  # Assuming IP address is in the third column
        channel.basic_publish(exchange='', routing_key='task_queue', body=switch_ip)
        logger.info(f"Switch IP {switch_ip} added to task queue.")

    connection.close()
except pika.exceptions.AMQPError as e:
    logger.error(f"Error connecting to RabbitMQ: {e}")

connection.close()