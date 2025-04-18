from dataclasses import dataclass
from typing import List, Optional

CAST_MEMBER_FIELDS = 8
CREW_MEMBER_FIELDS = 7

@dataclass
class CastMember:
    cast_id: int
    character: str
    credit_id: str
    gender: Optional[int]
    id: int
    name: str
    order: int
    profile_path: Optional[str]


@dataclass
class CrewMember:
    credit_id: str
    department: str
    gender: Optional[int]
    id: int
    job: str
    name: str
    profile_path: Optional[str]


@dataclass
class Credit:
    cast: List[CastMember]
    crew: List[CrewMember]
    id: int