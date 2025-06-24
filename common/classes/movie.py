from dataclasses import dataclass
from typing import List, Optional

from common.logger import get_logger

logger = get_logger("Movie")


@dataclass
class BelongsToCollection:
    id: int
    name: str
    poster_path: str
    backdrop_path: str


@dataclass
class Genre:
    id: int
    name: str


@dataclass
class ProductionCompany:
    name: str
    id: int


@dataclass
class ProductionCountry:
    iso_3166_1: str
    name: str


@dataclass
class SpokenLanguage:
    iso_639_1: str
    name: str


MOVIE_LINE_FIELDS = 24


@dataclass
class Movie:
    adult: bool
    belongs_to_collection: Optional[BelongsToCollection]
    budget: Optional[int]
    genres: Optional[List[Genre]]
    homepage: Optional[str]
    id: int
    imdb_id: Optional[str]
    original_language: str
    original_title: str
    overview: str
    popularity: float
    poster_path: Optional[str]
    production_companies: Optional[List[ProductionCompany]]
    production_countries: Optional[List[ProductionCountry]]
    release_date: str
    revenue: Optional[float]
    runtime: Optional[float]
    spoken_languages: Optional[List[SpokenLanguage]]
    status: str
    tagline: Optional[str]
    title: str
    video: bool
    vote_average: float
    vote_count: float

    def log_movie_info(self) -> None:
        movie_id = self.id
        title = self.title
        genres = [genre.name for genre in self.genres] if self.genres else []
        budget = self.budget if self.budget is not None else 0
        revenue = self.revenue if self.revenue is not None else 0.0
        release_date = self.release_date or "Unknown"
        countries = (
            [country.name for country in self.production_countries]
            if self.production_countries
            else []
        )
        overview_short = (
            (self.overview[:75] + "...")
            if self.overview and len(self.overview) > 75
            else (self.overview or "")
        )

        logger.debug("Movie Information:")
        logger.debug(f"\tID: {movie_id}")
        logger.debug(f"\tTitle: {title}")
        logger.debug(f"\tGenres: {', '.join(genres)}")
        logger.debug(f"\tBudget: ${budget}")
        logger.debug(f"\tRevenue: ${revenue}")
        logger.debug(f"\tRelease Date: {release_date}")
        logger.debug(f"\tProduction Countries: {', '.join(countries)}")
        logger.debug(f"\tOverview: {overview_short}")
