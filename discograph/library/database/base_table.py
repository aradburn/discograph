from typing import TypeVar

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


ConcreteTable = TypeVar("ConcreteTable", bound=Base)
