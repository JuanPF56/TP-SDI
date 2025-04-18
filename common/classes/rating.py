from dataclasses import dataclass

@dataclass
class Rating:
    userId: int
    movieId: int
    rating: float
    timestamp: int