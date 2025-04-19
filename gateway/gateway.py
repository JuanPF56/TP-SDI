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
            "production_countries": ["United States of America"],
            "genres": ["Action", "Science Fiction"]
        },
        {
            "title": "The Matrix",
            "release_date": "1999-03-31",
            "budget": 63000000,
            "revenue": 463517383,
            "production_countries": ["United States of America"],
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
        "title": "Interstellar",
        "release_date": "2014-11-07",
        "budget": 165000000,
        "revenue": 677471339,
        "production_countries": ["United States of America"],
        "genres": ["Adventure", "Drama", "Science Fiction"]
    },
    {
        "title": "The Dark Knight",
        "release_date": "2008-07-18",
        "budget": 185000000,
        "revenue": 1004558444,
        "production_countries": ["United States of America"],
        "genres": ["Action", "Crime", "Drama"]
    },
    {
        "title": "Pulp Fiction",
        "release_date": "1994-10-14",
        "budget": 8000000,
        "revenue": 213928762,
        "production_countries": ["United States of America"],
        "genres": ["Crime", "Drama"]
    },
    {
        "title": "Fight Club",
        "release_date": "1999-10-15",
        "budget": 63000000,
        "revenue": 101209702,
        "production_countries": ["United States of America"],
        "genres": ["Drama"]
    },
    {
        "title": "Forrest Gump",
        "release_date": "1994-07-06",
        "budget": 55000000,
        "revenue": 678226465,
        "production_countries": ["United States of America"],
        "genres": ["Comedy", "Drama", "Romance"]
    },
    {
        "title": "The Shawshank Redemption",
        "release_date": "1994-09-23",
        "budget": 25000000,
        "revenue": 28341469,
        "production_countries": ["United States of America"],
        "genres": ["Drama", "Crime"]
    },
    {
        "title": "The Godfather",
        "release_date": "1972-03-24",
        "budget": 6000000,
        "revenue": 246120974,
        "production_countries": ["United States of America"],
        "genres": ["Crime", "Drama"]
    },
    {
        "title": "The Lord of the Rings: The Fellowship of the Ring",
        "release_date": "2001-12-19",
        "revenue": 871530324,
        "production_countries": ["New Zealand", "United States of America"],
        "genres": ["Adventure", "Fantasy", "Action"]
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
