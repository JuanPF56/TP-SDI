from typing import List
import ast

from common.classes.movie import Movie, BelongsToCollection, Genre, ProductionCompany, ProductionCountry, SpokenLanguage, MOVIE_LINE_FIELDS
from common.classes.credit import Credit, CastMember, CrewMember, CAST_MEMBER_FIELDS, CREW_MEMBER_FIELDS

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
        self._cast_buffer = []
        self._crew_buffer = []

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
        completed_credits = []

        data = self._partial_data + decoded_payload
        lines = data.split('\n')

        # Buffer temporal: la última línea puede estar incompleta
        if data and not data.endswith('\n'):
            self._partial_data = lines.pop()  # guardar la línea incompleta
        else:
            self._partial_data = ""

        for line in lines:
            fields = line.strip().split('"')
            if not fields or fields == ['']:
                continue  # línea vacía, saltear

            if len(fields) == CAST_MEMBER_FIELDS:
                cast_member = CastMember(
                    cast_id=int(fields[0]),
                    character=fields[1],
                    credit_id=fields[2],
                    gender=int(fields[3]) if fields[3] else None,
                    id=int(fields[4]),
                    name=fields[5],
                    order=int(fields[6]),
                    profile_path=fields[7] if fields[7] else None
                )
                self._cast_buffer.append(cast_member)

            elif len(fields) == CREW_MEMBER_FIELDS:
                crew_member = CrewMember(
                    credit_id=fields[0],
                    department=fields[1],
                    gender=int(fields[2]) if fields[2] else None,
                    id=int(fields[3]),
                    job=fields[4],
                    name=fields[5],
                    profile_path=fields[6] if fields[6] else None
                )
                self._crew_buffer.append(crew_member)

            elif len(fields) == 1:
                try:
                    credit_id = int(fields[0])
                    credit = Credit(
                        cast=self._cast_buffer,
                        crew=self._crew_buffer,
                        id=credit_id
                    )
                    completed_credits.append(credit)
                    self._cast_buffer = []  # resetear buffers
                    self._crew_buffer = []
                except ValueError:
                    # Si no es un int válido, quizás es una línea corrupta
                    continue

            else:
                # Línea desconocida o corrupta: la podés loguear o ignorar
                continue

        return completed_credits

    def decode_ratings(self, data: str) -> list:
        """
        Decode ratings from the data string
        """
        # Implement the decoding logic here
        return data.split(",")