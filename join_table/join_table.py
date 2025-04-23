import os
import time
import pika
import json
import configparser
from common.logger import get_logger

logger = get_logger("Join-Table")
EOS_TYPE = "EOS"

def load_config():
    config = configparser.ConfigParser()
    try:
        config.read("config.ini")
    except FileNotFoundError:
        logger.error("Error: config.ini not found.")
        raise

    rabbitmq_host = config["DEFAULT"].get("rabbitmq_host", "rabbitmq")
    input_queue = config["DEFAULT"].get("movies_arg_post_2000_queue", "movies_arg_post_2000")
    broadcast_exchange = config["DEFAULT"].get("movies_table_exchange", "movies_table_broadcast")

    return {
        "rabbitmq_host": rabbitmq_host,
        "input_queue": input_queue,
        "broadcast_exchange": broadcast_exchange,
    }

def setup_rabbitmq_connection(rabbitmq_host):
    delay = 2
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
            channel = connection.channel()
            logger.info("Connected to RabbitMQ")
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            logger.error("RabbitMQ connection error. Retrying in %d seconds...", delay)
            time.sleep(delay)
            delay = min(delay * 2, 30)

def main():
    config = load_config()
    connection, channel = setup_rabbitmq_connection(config["rabbitmq_host"])

    input_queue = config["input_queue"]
    broadcast_exchange = config["broadcast_exchange"]

    channel.exchange_declare(exchange=broadcast_exchange, exchange_type='fanout')
    channel.queue_declare(queue=input_queue)

    movies = []

    def callback(ch, method, properties, body):
        nonlocal movies
        msg_type = properties.type if properties and properties.type else "UNKNOWN"

        if msg_type == EOS_TYPE:
            logger.info("Received EOS message.")
            if len(movies) > 0:            
                channel.basic_publish(
                    exchange=broadcast_exchange,
                    routing_key='',
                    body=json.dumps(movies).encode('utf-8')
                )
                logger.info("Sent table of %d movies", len(movies))
            ch.stop_consuming()
        else:
            message = json.loads(body)
            logger.info(f"Received message: {message}")
            movies.extend(message)

    logger.info("Consuming from queue: %s", input_queue)
    channel.basic_consume(queue=input_queue, on_message_callback=callback, auto_ack=True)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.warning("Interrupted manually")
    finally:
        connection.close()
        logger.info("Connection closed")

if __name__ == "__main__":
    main()
