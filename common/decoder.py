from typing import List
import ast
import re

from common.classes.movie import Movie, BelongsToCollection, Genre, ProductionCompany, ProductionCountry, SpokenLanguage, MOVIE_LINE_FIELDS
from common.classes.credit import Credit, parse_raw_cast_data, parse_raw_crew_data
from common.classes.rating import Rating, RATING_LINE_FIELDS

from common.logger import get_logger
logger = get_logger("Decoder")

class Decoder:
    """
    Decoder class to decode messages from the protocol gateway
    """
    def __init__(self):
        self._decoded_movies = 0
        self._decoded_credits = 0
        self._decoded_ratings = 0

        self._partial_data = ""

    def get_decoded_movies(self) -> int:
        """
        Get the number of decoded movies
        """
        return self._decoded_movies
    
    def get_decoded_credits(self) -> int:
        """
        Get the number of decoded credits
        """
        return self._decoded_credits
    
    def get_decoded_ratings(self) -> int:
        """
        Get the number of decoded ratings
        """
        return self._decoded_ratings

    def decode_movies(self, data: str) -> List[Movie]:
        movies = []
        lines = data.strip().split("\n")

        for line in lines:
            fields = line.split("\0")
            if len(fields) < MOVIE_LINE_FIELDS:
                logger.warning(f"Ignoring line with insufficient fields")
                continue

            try:
                adult = fields[0] == "True"

                # Belongs to Collection
                belongs_to_collection = None
                if fields[1]:
                    collection_data = ast.literal_eval(fields[1])
                    belongs_to_collection = BelongsToCollection(
                        id=int(collection_data['id']),
                        name=collection_data['name'],
                        poster_path=collection_data['poster_path'],
                        backdrop_path=collection_data['backdrop_path']
                    )

                budget = int(fields[2]) if fields[2] else None

                # Genres
                genres = []
                if fields[3]:
                    genres_data = ast.literal_eval(fields[3])
                    genres = [Genre(id=int(g['id']), name=g['name']) for g in genres_data]

                homepage = fields[4]
                movie_id = int(fields[5])
                imdb_id = fields[6]
                original_language = fields[7]
                original_title = fields[8]
                overview = fields[9]
                popularity = float(fields[10])
                poster_path = fields[11]

                # Production Companies
                production_companies = []
                if fields[12]:
                    companies_data = ast.literal_eval(fields[12])
                    production_companies = [ProductionCompany(name=pc['name'], id=pc['id']) for pc in companies_data]

                # Production Countries
                production_countries = []
                if fields[13]:
                    countries_data = ast.literal_eval(fields[13])
                    production_countries = [ProductionCountry(iso_3166_1=pc['iso_3166_1'], name=pc['name']) for pc in countries_data]

                release_date = fields[14]
                revenue = float(fields[15]) if fields[15] else None
                runtime = float(fields[16]) if fields[16] else None

                # Spoken Languages
                spoken_languages = []
                if fields[17]:
                    langs_data = ast.literal_eval(fields[17])
                    spoken_languages = [SpokenLanguage(iso_639_1=sl['iso_639_1'], name=sl['name']) for sl in langs_data]

                status = fields[18]
                tagline = fields[19]
                title = fields[20]
                video = fields[21] == "True"
                vote_average = float(fields[22])
                vote_count = float(fields[23])

                movie = Movie(
                    adult=adult,
                    belongs_to_collection=belongs_to_collection,
                    budget=budget,
                    genres=genres,
                    homepage=homepage,
                    id=movie_id,
                    imdb_id=imdb_id,
                    original_language=original_language,
                    original_title=original_title,
                    overview=overview,
                    popularity=popularity,
                    poster_path=poster_path,
                    production_companies=production_companies,
                    production_countries=production_countries,
                    release_date=release_date,
                    revenue=revenue,
                    runtime=runtime,
                    spoken_languages=spoken_languages,
                    status=status,
                    tagline=tagline,
                    title=title,
                    video=video,
                    vote_average=vote_average,
                    vote_count=vote_count
                )

                movies.append(movie)
                self._decoded_movies += 1

            except Exception as e:
                logger.error(f"Error decoding line: {line}, error: {e}")
                continue

        return movies
    
    def decode_credits(self, decoded_payload: str) -> List[Credit]:
        logger.debug(f"Decoding credits from payload: {decoded_payload}")
        logger.debug("Decoding credits")
        completed_credits = []

        logger.debug(f"PARTIAL DATA INICIO: {self._partial_data}")

        data_acumulated = self._partial_data + decoded_payload

        # Check if there is a \n in data_acumulated, else skip processing
        if '\n' not in data_acumulated:
            logger.debug("No complete lines to process, waiting for more data")
            self._partial_data = data_acumulated  # Save the incomplete data
            return completed_credits

        # Split the data into lines (at least one line is complete)
        lines = data_acumulated.split('\n')
        logger.debug(f"Data split into {len(lines)} lines")
        for line in lines:
            logger.debug(f"Line: {line}")

        # Last line may be incomplete, save it in _partial_data
        self._partial_data = lines[-1]
        logger.debug(f"PARTIAL DATA SAVED: {self._partial_data}")
        complete_lines = lines[:-1]  # all lines except the last one

        for line in complete_lines:
            line = line.strip()
            if not line:
                logger.debug("Empty line, skipping")
                continue  # saltar líneas vacías

            try:
                logger.debug("SPLITTING LINE")
                regex = r'^(?P<cast_data>".*?"|\[\]),(?P<crew_data>".*?"|\[\]),(?P<id>\d+)$'
                match = re.match(regex, line)

                if not match:
                    raise ValueError("Line format does not match expected structure.")

                # --- Cast ---
                raw_cast_data = match.group("cast_data")
                logger.debug(f"Raw cast data: {raw_cast_data}")

                clean_cast_data = raw_cast_data.strip('"')
                if clean_cast_data and clean_cast_data != '[]':
                    cast_members = parse_raw_cast_data(clean_cast_data)
                    for cast_member in cast_members:
                        cast_member.log_cast_member_info()
                else:
                    cast_members = []

                # --- Crew ---
                raw_crew_data = match.group("crew_data")
                logger.debug(f"Raw crew data: {raw_crew_data}")

                clean_crew_data = raw_crew_data.strip('"')
                if clean_crew_data and clean_crew_data != '[]':
                    crew_members = parse_raw_crew_data(clean_crew_data)
                    for crew_member in crew_members:
                        crew_member.log_crew_member_info()
                else:
                    crew_members = []

                # --- ID ---
                credit_id = int(match.group("id"))
                logger.debug(f"Credit ID: {credit_id}")

                credit = Credit(
                    cast=cast_members,
                    crew=crew_members,
                    id=credit_id
                )
                completed_credits.append(credit)
                self._decoded_credits += 1

            except (TypeError, ValueError) as e:
                logger.error(f"Error decoding line: {line} -> {e}")
                continue

        return completed_credits

    def decode_ratings(self, data: str) -> list[Rating]:
        """
        Decode ratings from the data string
        """
        ratings = []
        lines = data.strip().split("\n")
        
        for line in lines:
            fields = line.split("\0")
            if len(fields) != RATING_LINE_FIELDS:
                logger.warning(f"Ignoring line with insufficient fields")
                continue

            user_id, movie_id, rating, timestamp = fields
            ratings.append(Rating(
                userId=int(user_id),
                movieId=int(movie_id),
                rating=float(rating),
                timestamp=int(timestamp)
            ))
            self._decoded_ratings += 1

        return ratings