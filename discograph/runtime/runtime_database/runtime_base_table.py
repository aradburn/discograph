from typing import TypeVar

from sqlalchemy.orm import DeclarativeBase


class RuntimeBase(DeclarativeBase):
    pass


RuntimeConcreteTable = TypeVar("RuntimeConcreteTable", bound=RuntimeBase)
