import configparser
import json
import pika

from common.logger import get_logger

logger = get_logger("Gateway")
TEST_DATA = {
    "movies_raw": [
        {
            "title": "Inception",
            "release_date": "2010-07-16",
            "budget": 160000000,
            "revenue": 829895144,
            "production_countries": [{"iso_3166_1": "US", "name": "United States of America"}],
            "genres": ["Action", "Science Fiction"]
        },
        {
            "title": "The Matrix",
            "release_date": "1999-03-31",
            "budget": 63000000,
            "revenue": 463517383,
            "production_countries": [{"iso_3166_1": "US", "name": "United States of America"}],
            "genres": ["Action", "Science Fiction"]
        }
    ],
    "ratings_raw": [
        {
            "userId": 1,
            "movieId": 101,
            "rating": 4.5
        },
        {
            "userId": 2,
            "movieId": 102,
            "rating": 5.0
        }
    ],
    "credits_raw": [
        {
            "movieId": 101,
            "cast": ["Leonardo DiCaprio", "Joseph Gordon-Levitt"],
            "crew": [{"name": "Christopher Nolan", "job": "Director"}]
        },
        {
            "movieId": 102,
            "cast": ["Keanu Reeves", "Laurence Fishburne"],
            "crew": [{"name": "Lana Wachowski", "job": "Director"}, {"name": "Lilly Wachowski", "job": "Director"}]
        }
    ]
}
TEST_DATA["movies_raw"].extend([
    {
        "title": "The Secret in Their Eyes",
        "release_date": "2009-08-13",
        "budget": 2000000,
        "revenue": 34000000,
        "production_countries": [{"iso_3166_1": "AR", "name": "Argentina"}],
        "genres": ["Crime", "Drama", "Mystery"]
    },
    {
        "title": "Wild Tales",
        "release_date": "2014-08-21",
        "budget": 3300000,
        "revenue": 30000000,
        "production_countries": [{"iso_3166_1": "AR", "name": "Argentina"}],
        "genres": ["Comedy", "Drama", "Thriller"]
    },
    {
        "title": "The Clan",
        "release_date": "2015-08-13",
        "budget": 4000000,
        "revenue": 21000000,
        "production_countries": [{"iso_3166_1": "AR", "name": "Argentina"}],
        "genres": ["Crime", "Drama", "Thriller"]
    },
    {
        "title": "Nine Queens",
        "release_date": "2000-08-31",
        "budget": 1200000,
        "revenue": 12000000,
        "production_countries": [{"iso_3166_1": "AR", "name": "Argentina"}],
        "genres": ["Crime", "Drama", "Thriller"]
    },
    {
        "title": "The Motorcycle Diaries",
        "release_date": "2004-09-24",
        "budget": 4000000,
        "revenue": 57000000,
        "production_countries": [{"iso_3166_1": "AR", "name": "Argentina"}],
        "genres": ["Adventure", "Biography", "Drama"]
    },
    {
        "title": "The Official Story",
        "release_date": "1985-04-03",
        "budget": 800000,
        "revenue": 4000000,
        "production_countries": [
            {"iso_3166_1": "AR", "name": "Argentina"},
            {"iso_3166_1": "ES", "name": "Spain"}
        ],
        "genres": ["Drama", "History"]
    },
    {
        "title": "Kóblic",
        "release_date": "2016-04-14",
        "budget": 2000000,
        "revenue": 3000000,
        "production_countries": [
            {"iso_3166_1": "AR", "name": "Argentina"},
            {"iso_3166_1": "ES", "name": "Spain"}
        ],
        "genres": ["Crime", "Drama", "Thriller"]
    },
    {
        "title": "Everybody Knows",
        "release_date": "2018-05-09",
        "budget": 11000000,
        "revenue": 18000000,
        "production_countries": [
            {"iso_3166_1": "AR", "name": "Argentina"},
            {"iso_3166_1": "ES", "name": "Spain"}
        ],
        "genres": ["Crime", "Drama", "Mystery"]
    }
])

def load_config():
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config

def send_test_messages(config):
    rabbitmq_host = config["DEFAULT"].get("rabbitmq_host", "rabbitmq")
    for _ in range(10):  
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
            channel = connection.channel()
            print("Connected to RabbitMQ")
            break  
        except pika.exceptions.AMQPConnectionError:
            print("Waiting for RabbitMQ to be available...")
    else:
        print("Failed to connect to RabbitMQ after 10 attempts")
        return

    queues = {
        "movies_raw": config["DEFAULT"].get("movies_raw_queue", "movies_raw"),
        "ratings_raw": config["DEFAULT"].get("ratings_raw_queue", "ratings_raw"),
        "credits_raw": config["DEFAULT"].get("credits_raw_queue", "credits_raw"),
    }

    for key, queue_name in queues.items():
        channel.queue_declare(queue=queue_name)
        for item in TEST_DATA[key]:
            message = json.dumps(item)
            channel.basic_publish(exchange='', routing_key=queue_name, body=message)
            logger.info(f"Sent test message to '{queue_name}': {message}")

    connection.close()

def main():
    config = load_config()
    logger.info("Gateway node is online")
    logger.info("Configuration loaded successfully")
    for key, value in config["DEFAULT"].items():
        logger.info(f"{key}: {value}")

    send_test_messages(config)

if __name__ == "__main__":
    main()
