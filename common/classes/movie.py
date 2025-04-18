from dataclasses import dataclass

MOVIE_LINE_FIELDS = 24

@dataclass
class Movie:
    adult: str
    belongs_to_collection: str
    budget: str
    genres: str
    homepage: str
    id: str
    imdb_id: str
    original_language: str
    original_title: str
    overview: str
    popularity: str
    poster_path: str
    production_companies: str
    production_countries: str
    release_date: str
    revenue: str
    runtime: str
    spoken_languages: str
    status: str
    tagline: str
    title: str
    video: str
    vote_average: str
    vote_count: str
