import os
import csv
import ast

from common.logger import get_logger
logger = get_logger("Client")

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

def send_movies(dataset_path, protocol):
    csv_path = os.path.join(dataset_path, "movies_metadata.csv")
    try:
        with open(csv_path, newline='', encoding="utf-8") as csvfile:
            reader = list(csv.reader(csvfile))
            headers = reader[0]  # skip header
            rows = reader[1:]

            total_lines = len(rows)
            protocol.send_amount_of_lines(total_lines)
            logger.info(f"Sending {total_lines} movie rows...")

            for row in rows:
                line = ",".join(row)
                protocol.send_csv_line(line)
    except Exception as e:
        logger.error(f"Error reading/sending CSV: {e}")