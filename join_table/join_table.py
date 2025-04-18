import pika
import json # Just to test RabbitMQ
import configparser
from common.logger import get_logger

logger = get_logger("Join-Table")

# Test batches

movies = {
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
}

def load_config():
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config

def main():
    config = load_config()
    logger.info("Join Table node is online")

    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()

    

    # Declare a fanout exchange
    channel.exchange_declare(exchange='broadcast', exchange_type='fanout')
    # Publish a message to the exchange
    # TODO: Serialize with our own protocol
    channel.basic_publish(exchange='broadcast', routing_key='', body=json.dumps(movies))

    print(" [x] Sent broadcast")
    connection.close()

if __name__ == "__main__":
    main()
