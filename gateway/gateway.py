import configparser
import json
import pika
import csv
import ast
import os

from common.logger import get_logger

logger = get_logger("Gateway")
""" TEST_DATA = {
    "movies_raw": [
        {
            "title": "Inception",
            "release_date": "2010-07-16",
            "budget": 160000000,
            "revenue": 829895144,
            "production_countries": [{"iso_3166_1": "US", "name": "United States of America"}],
            "genres": [{"id": 28, "name": "Action"}, {"id": 878, "name": "Science Fiction"}]
        },
        {
            "title": "The Matrix",
            "release_date": "1999-03-31",
            "budget": 63000000,
            "revenue": 463517383,
            "production_countries": [{"iso_3166_1": "US", "name": "United States of America"}],
            "genres": [{"id": 28, "name": "Action"}, {"id": 878, "name": "Science Fiction"}]
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
        "genres": [{"id": 80, "name": "Crime"}, {"id": 18, "name": "Drama"}, {"id": 9648, "name": "Mystery"}]
    },
    {
        "title": "Wild Tales",
        "release_date": "2014-08-21",
        "budget": 3300000,
        "revenue": 30000000,
        "production_countries": [{"iso_3166_1": "AR", "name": "Argentina"}],
        "genres": [{"id": 35, "name": "Comedy"}, {"id": 18, "name": "Drama"}, {"id": 53, "name": "Thriller"}]
    },
    {
        "title": "The Clan",
        "release_date": "2015-08-13",
        "budget": 4000000,
        "revenue": 21000000,
        "production_countries": [{"iso_3166_1": "AR", "name": "Argentina"}],
        "genres": [{"id": 80, "name": "Crime"}, {"id": 18, "name": "Drama"}, {"id": 53, "name": "Thriller"}]
    },
    {
        "title": "Nine Queens",
        "release_date": "2000-08-31",
        "budget": 1200000,
        "revenue": 12000000,
        "production_countries": [{"iso_3166_1": "AR", "name": "Argentina"}],
        "genres": [{"id": 80, "name": "Crime"}, {"id": 18, "name": "Drama"}, {"id": 53, "name": "Thriller"}]
    },
    {
        "title": "The Motorcycle Diaries",
        "release_date": "2004-09-24",
        "budget": 4000000,
        "revenue": 57000000,
        "production_countries": [{"iso_3166_1": "AR", "name": "Argentina"}],
        "genres": [{"id": 12, "name": "Adventure"}, {"id": 18, "name": "Biography"}, {"id": 18, "name": "Drama"}]
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
        "genres": [{"id": 18, "name": "Drama"}, {"id": 36, "name": "History"}]
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
        "genres": [{"id": 80, "name": "Crime"}, {"id": 18, "name": "Drama"}, {"id": 53, "name": "Thriller"}]
    },
    {
        "title": "Everybody Knows",
        "release_date": "2001-05-09",
        "budget": 11000000,
        "revenue": 18000000,
        "production_countries": [
            {"iso_3166_1": "AR", "name": "Argentina"},
            {"iso_3166_1": "ES", "name": "Spain"}
        ],
        "genres": [{"id": 80, "name": "Crime"}, {"id": 18, "name": "Drama"}, {"id": 9648, "name": "Mystery"}]
    }
]) """
def load_config():
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config
def send_movies_from_csv(config):
    file_path = os.path.join("client", "data", "movies.csv")
    rabbitmq_host = config["DEFAULT"].get("rabbitmq_host", "rabbitmq")

    for _ in range(10):  
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
            channel = connection.channel()
            logger.info("Connected to RabbitMQ")
            break  
        except pika.exceptions.AMQPConnectionError:
            logger.warning("Waiting for RabbitMQ to be available...")
    else:
        logger.error("Failed to connect to RabbitMQ after 10 attempts")
        return

    movies_raw_queue = config["DEFAULT"].get("movies_raw_queue", "movies_raw")
    channel.queue_declare(queue=movies_raw_queue)

    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            try:
                movie = {
                    "title": row["title"],
                    "release_date": row["release_date"],
                    "budget": int(row["budget"]) if row["budget"] else 0,
                    "revenue": int(row["revenue"]) if row["revenue"] else 0,
                    "production_countries": ast.literal_eval(row["production_countries"]) if row["production_countries"] else [],
                    "genres": ast.literal_eval(row["genres"]) if row["genres"] else []
                }

                message = json.dumps(movie)
                channel.basic_publish(
                    exchange='',
                    routing_key=movies_raw_queue,
                    body=message,
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # make message persistent
                    )
                )
                logger.info(f"Sent movie: {movie['title']}")
            except Exception as e:
                logger.error(f"Failed to process row: {row['title'] if 'title' in row else 'Unknown'} - {e}")

    connection.close()

def main():
    config = load_config()
    logger.info("Gateway node is online")
    logger.info("Configuration loaded successfully")
    for key, value in config["DEFAULT"].items():
        logger.info(f"{key}: {value}")

    send_movies_from_csv(config)

if __name__ == "__main__":
    main()
