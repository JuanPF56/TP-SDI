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
    delay = 2  # Initial delay
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
            channel = connection.channel()
            logger.info("Connected to RabbitMQ")
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            logger.error("RabbitMQ connection error. Retrying in 5 seconds...")
            time.sleep(delay)
            delay = min(delay * 2, 30)  # Exponential backoff with cap

def receive_movie_batches(channel, input_queue, batch_size=100):
    batch = []
    done = False

    def callback(ch, method, properties, body):
        nonlocal done, batch
        msg_type = properties.type if properties and properties.type else "UNKNOWN"

        if msg_type == EOS_TYPE:
            logger.info("Received EOS message.")
            if batch:
                yield_batch = batch[:]
                batch.clear()
                yield yield_batch
            done = True
            channel.stop_consuming()
        else:
            message = json.loads(body)
            logger.debug(f"Received message: {message}")
            batch.append(message)
            if len(batch) >= batch_size:
                yield_batch = batch[:]
                batch.clear()
                yield yield_batch

    while not done:
        try:
            channel.basic_consume(queue=input_queue, on_message_callback=callback, auto_ack=True)
            channel.start_consuming()
        except Exception as e:
            logger.error(f"Error during consuming: {e}")
            break

def main():
    config = load_config()
    connection, channel = setup_rabbitmq_connection(config["rabbitmq_host"])

    broadcast_exchange = config["broadcast_exchange"]
    input_queue = config["input_queue"]

    # Declare exchange and queue
    channel.exchange_declare(exchange=broadcast_exchange, exchange_type='fanout')
    channel.queue_declare(queue=input_queue)


    # Process and send batches
    for batch in receive_movie_batches(channel, input_queue):
        data = {
            "movies": batch,
            "last": False
        }
        logger.info(f"Publishing batch of {len(batch)} movies")
        channel.basic_publish(
            exchange=broadcast_exchange,
            routing_key='',
            body=json.dumps(data).encode('utf-8')
        )

    # Send final 'last' message
    final_msg = {
        "movies": [],
        "last": True
    }
    channel.basic_publish(
        exchange=broadcast_exchange,
        routing_key='',
        body=json.dumps(final_msg).encode('utf-8')
    )
    logger.info("All movie batches sent. Sent final 'last=True' message.")

    connection.close()

if __name__ == "__main__":
    main()
