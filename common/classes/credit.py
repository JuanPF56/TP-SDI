from dataclasses import dataclass
from typing import List, Optional
import ast
import re

from common.logger import get_logger
logger = get_logger("Credit")

CAST_MEMBER_FIELDS = 8
CREW_MEMBER_FIELDS = 7

def fix_double_quotes_inside_json(raw: str) -> str:
    return re.sub(r'""([^"{}]+?)""', r'"\1"', raw)

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

    def log_cast_member_info(self) -> None:
        logger.debug("Cast Member Information:")
        logger.debug(f"\tID: {self.id}")
        logger.debug(f"\tName: {self.name}")
        logger.debug(f"\tCharacter: {self.character}")

def parse_raw_cast_data(raw_data: str) -> List[CastMember]:
    cast_members = []
    # Extrae cada objeto {...} individual usando regex
    parts = re.findall(r'\{.*?\}', raw_data)
    
    for i, part in enumerate(parts):
        try:
            cleaned_part = fix_double_quotes_inside_json(part)
            data_dict = ast.literal_eval(cleaned_part)
            cast_member = CastMember(**data_dict)
            cast_members.append(cast_member)
        except Exception as e:
            logger.error(f"Error parsing cast part {i}: {e}\nPart content: {part}")

    return cast_members

@dataclass
class CrewMember:
    credit_id: str
    department: str
    gender: Optional[int]
    id: int
    job: str
    name: str
    profile_path: Optional[str]

    def log_crew_member_info(self) -> None:
        logger.debug("Crew Member Information:")
        logger.debug(f"\tID: {self.id}")
        logger.debug(f"\tName: {self.name}")
        logger.debug(f"\tJob: {self.job}")
        logger.debug(f"\tDepartment: {self.department}")

def parse_raw_crew_data(raw_data: str) -> List[CrewMember]:
    crew_members = []
    # Extrae cada objeto {...} individual usando regex
    parts = re.findall(r'\{.*?\}', raw_data)
    
    for i, part in enumerate(parts):
        try:
            cleaned_part = fix_double_quotes_inside_json(part)
            data_dict = ast.literal_eval(cleaned_part)
            crew_member = CrewMember(**data_dict)
            crew_members.append(crew_member)
        except Exception as e:
            logger.error(f"Error parsing crew part {i}: {e}\nPart content: {part}")

    return crew_members

CREDITS_LINE_FIELDS = 3
@dataclass
class Credit:
    cast: List[CastMember]
    crew: List[CrewMember]
    id: int

    def log_credit_info(self) -> None:
        logger.debug("Credit Information:")
        logger.debug(f"\tID: {self.id}")
        logger.debug(f"\tCast Members: {len(self.cast)}")
        for cast_member in self.cast:
            logger.debug(f"\t\tName: {cast_member.name}, Character: {cast_member.character}")
        logger.debug(f"\tCrew Members: {len(self.crew)}")
        for crew_member in self.crew:
            logger.debug(f"\t\tName: {crew_member.name}, Job: {crew_member.job}")
 