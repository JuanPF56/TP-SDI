import os
import time
import pika
import json
import configparser
from common.logger import get_logger

logger = get_logger("Join-Table")
EOS_TYPE = "EOS" 

def load_config():
    # Load the config file
    config = configparser.ConfigParser()
    try:
        config.read("config.ini")
    except FileNotFoundError:
        logger.error("Error: config.ini not found.")
        raise

    # Get RabbitMQ host from the config file
    rabbitmq_host = config["DEFAULT"].get("rabbitmq_host", "rabbitmq")

    # Get queue names from the config file
    input_queue = config["DEFAULT"].get("movies_arg_post_2000_queue", "movies_arg_post_2000")
    broadcast_exchange = config["DEFAULT"].get("movies_table_exchange", "movies_table_broadcast")
    jb_ready_queue = config["DEFAULT"].get("join_batch_ready_queue", "join_batch_ready")

    return {
        "rabbitmq_host": rabbitmq_host,
        "input_queue": input_queue,
        "broadcast_exchange": broadcast_exchange,
        "jb_ready_queue": jb_ready_queue
    }

def setup_rabbitmq_connection(rabbitmq_host):
    # Establish a connection to RabbitMQ
    delay = 2  # Seconds
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
            channel = connection.channel()
            logger.info("Connected to RabbitMQ")
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            logger.error("RabbitMQ connection error. Retrying in 5 seconds...")
            time.sleep(2)
            delay *= 2 # Exponential backoff

def receive_movies_table(channel, input_queue):
    movies_table = []
    done = False

    def callback(ch, method, properties, body):
        nonlocal done
        msg_type = properties.type if properties and properties.type else "UNKNOWN"
        if msg_type == EOS_TYPE:
            logger.info("Received EOS message, stopping consumption.")    
            done = True
            channel.stop_consuming()
        else:
            message = json.loads(body)
            logger.debug(f"Received message: {message}")
            movies_table.append(message)

    channel.basic_consume(queue=input_queue, on_message_callback=callback, auto_ack=True)
    logger.info("Waiting for movies table...")

    while not done:
        try:
            channel.start_consuming()
        except Exception as e:
            logger.error(f"Error during consuming: {e}")
            break

    return movies_table

def main():
    config = load_config()
    connection, channel = setup_rabbitmq_connection(config["rabbitmq_host"])

    broadcast_exchange = config["broadcast_exchange"]
    input_queue = config["input_queue"]

    # Declare a fanout exchange
    channel.exchange_declare(exchange=broadcast_exchange, exchange_type='fanout')
    # Declare a queue for the input data
    channel.queue_declare(queue=input_queue)

    movies_table = receive_movies_table(channel, input_queue)
    logger.info("Received movies table: %s", movies_table)

    # Send the movies table to the join batch nodes
    logger.info("Sending movies table to join batch nodes...")
    
    # Publish a message to the exchange
    data = {
        "movies": movies_table,
        "last": True,
    }
    channel.basic_publish(
        exchange=broadcast_exchange,
        routing_key='',
        body=json.dumps(data).encode('utf-8')
    )

    logger.info(" [x] Sent broadcast")
    connection.close()

if __name__ == "__main__":
    main()
