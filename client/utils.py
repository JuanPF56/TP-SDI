import kagglehub
import os
import csv
import ast

from common.logger import get_logger
logger = get_logger("Client")

from protocol_client_gateway import ProtocolClient

ACK = 0
ERROR = 1

def download_dataset():
    try:
        logger.info("Downloading dataset with kagglehub...")
        path = kagglehub.dataset_download("rounakbanik/the-movies-dataset")
        logger.info(f"Dataset downloaded at: {path}")
        return path
    except Exception as e:
        logger.error(f"Failed to download dataset: {e}")
        return None

def read_first_3_movies(dataset_path):
    csv_path = os.path.join(dataset_path, "movies_metadata.csv")
    movies = []
    try:
        with open(csv_path, newline='', encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for _, row in zip(range(3), reader):
                movies.append(row)
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")
    return movies
    
def log_movies(movies):
    logger.debug("== First 3 movies from dataset ==")
    for i, movie in enumerate(movies, start=1):
        try:
            budget = movie.get("budget", "N/A")
            genres_str = movie.get("genres", "[]")
            genres = [g["name"] for g in ast.literal_eval(genres_str)]

            movie_id = movie.get("id", "N/A")
            title = movie.get("original_title", "N/A")

            overview = movie.get("overview", "").strip()
            overview_short = (overview[:150] + "...") if len(overview) > 150 else overview

            countries_str = movie.get("production_countries", "[]")
            countries = [c["name"] for c in ast.literal_eval(countries_str)]

            release_date = movie.get("release_date", "N/A")
            revenue = movie.get("revenue", "N/A")

            logger.debug(f"--- Movie {i} ---")
            logger.debug(f"ID: {movie_id}")
            logger.debug(f"Title: {title}")
            logger.debug(f"Genres: {', '.join(genres)}")
            logger.debug(f"Budget: ${budget}")
            logger.debug(f"Revenue: ${revenue}")
            logger.debug(f"Release Date: {release_date}")
            logger.debug(f"Production Countries: {', '.join(countries)}")
            logger.debug(f"Overview: {overview_short}")
        except Exception as e:
            logger.warning(f"Failed to log movie {i}: {e}")  

def send_datasets_to_server(datasets_path: str, protocol: ProtocolClient):
        protocol.send_dataset(datasets_path, "movies_metadata", "BATCH_MOVIES")
        receive_confirmation = protocol.receive_confirmation()
        if receive_confirmation != ACK:
            logger.error("Server returned an error after sending movies.")
            raise Exception("Server returned an error after sending movies.")
        logger.info("Movies were sent successfully.")

        protocol.send_dataset(datasets_path, "credits", "BATCH_ACTORS")
        receive_confirmation = protocol.receive_confirmation()
        if receive_confirmation != ACK:
            logger.error("Server returned an error after sending actors.")
            raise Exception("Server returned an error after sending actors.")
        logger.info("Actors were sent successfully.")


        protocol.send_dataset(datasets_path, "ratings", "BATCH_RATINGS")
        receive_confirmation = protocol.receive_confirmation()
        if receive_confirmation != ACK:
            logger.error("Server returned an error after sending ratings.")
            raise Exception("Server returned an error after sending ratings.")
        logger.info("Ratings were sent successfully.")

        logger.info("All datasets were sent.")
