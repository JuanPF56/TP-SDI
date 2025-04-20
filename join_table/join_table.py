import time
import pika
import json # Just to test RabbitMQ
import configparser
from common.logger import get_logger

logger = get_logger("Join-Table")

# Test batches

movies = [
    {
        "id": 1,
        "original_title": "El Secreto de Sus Ojos",
    },
    {
        "id": 2,
        "original_title": "Esperando la Carroza",
    },
    {
        "id": 3,
        "original_title": "Relatos Salvajes",
    },
    {
        "id": 4,
        "original_title": "Nueve Reinas",
    },
    {
        "id": 5,
        "original_title": "La Odisea de los Giles",
    },
    {
        "id": 6,
        "original_title": "Mi obra maestra",
    },        
]

def load_config():
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config

def main():
    config = load_config()
    logger.info("Join Table node is online")

    rabbitmq_host = config["DEFAULT"].get("rabbitmq_host", "rabbitmq")
    input_queue = config["DEFAULT"].get("movies_arg_post_2000_queue", "movies_arg_post_2000")
    broadcast_exchange = config["DEFAULT"].get("movies_table_exchange", "movies_table_broadcast")

    connection = None
    channel = None

    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
            channel = connection.channel()
            logger.info("Connected to RabbitMQ")
            break
        except pika.exceptions.AMQPConnectionError:
            time.sleep(5)

    # Declare a fanout exchange
    channel.exchange_declare(exchange=broadcast_exchange, exchange_type='fanout', durable=True)
    # Publish a message to the exchange
    data = {
        "movies": movies,
        "last": True,
    }
    channel.basic_publish(
        exchange=broadcast_exchange,
        routing_key='',
        body=json.dumps(data).encode('utf-8'),
        properties=pika.BasicProperties(
            delivery_mode=2  # Make message persistent
        )
    )

    logger.info(" [x] Sent broadcast")
    connection.close()

if __name__ == "__main__":
    main()
