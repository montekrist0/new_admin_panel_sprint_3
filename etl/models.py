from datetime import date
from typing import Optional, List
from uuid import UUID

import orjson

from pydantic import BaseModel


def orjson_dumps(v, *, default):
    return orjson.dumps(v, default=default).decode()


class IdMixin(BaseModel):
    id: UUID

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps


class Person(IdMixin):
    full_name: str
    role: List[str] = []
    film_ids: List[UUID] = []


class Genre(IdMixin):
    name: str


class Film(IdMixin):
    title: str
    description: str
    creation_date: date = None
    imdb_rating: Optional[float] = None
    genres: List[Genre] = []
    actors: List[Person] = []
    actors_names: List[str] = []
    directors: List[Person] = []
    writers: List[Person] = []
    writers_names: List[str] = []
