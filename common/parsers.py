"""Parsers for common data structures used in the application.
This module provides functions to parse dictionaries into structured data classes.
"""

from common.classes.movie import (
    Movie,
    BelongsToCollection,
    Genre,
    ProductionCompany,
    ProductionCountry,
    SpokenLanguage,
)


def parse_movie(movie_dict: dict) -> Movie:
    return Movie(
        adult=movie_dict["adult"],
        belongs_to_collection=(
            BelongsToCollection(**movie_dict["belongs_to_collection"])
            if movie_dict["belongs_to_collection"]
            else None
        ),
        budget=movie_dict.get("budget"),
        genres=[Genre(**g) for g in movie_dict.get("genres", [])],
        homepage=movie_dict.get("homepage"),
        id=movie_dict["id"],
        imdb_id=movie_dict.get("imdb_id"),
        original_language=movie_dict["original_language"],
        original_title=movie_dict["original_title"],
        overview=movie_dict["overview"],
        popularity=movie_dict["popularity"],
        poster_path=movie_dict.get("poster_path"),
        production_companies=[
            ProductionCompany(**p) for p in movie_dict.get("production_companies", [])
        ],
        production_countries=[
            ProductionCountry(**c) for c in movie_dict.get("production_countries", [])
        ],
        release_date=movie_dict["release_date"],
        revenue=movie_dict.get("revenue"),
        runtime=movie_dict.get("runtime"),
        spoken_languages=[
            SpokenLanguage(**s) for s in movie_dict.get("spoken_languages", [])
        ],
        status=movie_dict["status"],
        tagline=movie_dict.get("tagline"),
        title=movie_dict["title"],
        video=movie_dict["video"],
        vote_average=movie_dict["vote_average"],
        vote_count=movie_dict["vote_count"],
    )
