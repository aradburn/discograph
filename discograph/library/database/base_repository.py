import logging
from typing import Any, Generic, Type, Generator, cast

from sqlalchemy import asc, delete, desc, func, select, update, text
from sqlalchemy.engine import Result

__all__ = ("BaseRepository",)

from discograph.exceptions import UnprocessableError, DatabaseError, NotFoundError
from discograph.library.database.database_helper import ConcreteTable
from discograph.library.database.session import WrappedSession

log = logging.getLogger(__name__)


class BaseRepository(WrappedSession, Generic[ConcreteTable]):
    """This class implements the base interface for working with database
    and makes it easier to work with type annotations.

    The Session class implements the database interaction layer.
    """

    schema_class: Type[ConcreteTable]

    def __init__(self) -> None:
        super().__init__()

        if not self.schema_class:
            raise UnprocessableError(
                message="Can not initiate the class without schema_class attribute"
            )

    def _update(self, key: str, value: Any, payload: dict[str, Any]) -> ConcreteTable:
        """Updates an existed instance of the model in the related table.
        If some data is not exist in the payload then the null value will
        be passed to the schema class."""

        try:
            query = (
                update(self.schema_class)
                .where(
                    cast(
                        "ColumnElement[bool]", getattr(self.schema_class, key) == value
                    )
                )
                .values(payload)
                .returning(self.schema_class)
            )
            result: Result = self.execute(query)
            # result: Result = await self.execute(query)
            # self._session.flush()
            # await self._session.flush()
        except self._ERRORS:
            raise DatabaseError

        if not (schema := result.scalar_one_or_none()):
            raise DatabaseError

        return schema

    def _get(self, key: str, value: Any) -> ConcreteTable:
        """Return only one result by filters"""

        query = select(self.schema_class).where(
            cast("ColumnElement[bool]", getattr(self.schema_class, key) == value)
        )
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (_result := result.scalars().one_or_none()):
            raise NotFoundError

        return _result

    def count(self) -> int:
        query = select(func.count()).select_from(self.schema_class)
        result: Result = self.execute(query)
        # result: Result = await self.execute(func.count(self.schema_class.id))
        value = result.scalar()

        if not isinstance(value, int):
            raise UnprocessableError(
                message=(
                    "For some reason count function returned not an integer."
                    f"Value: {value}"
                ),
            )

        return value

    def _first(self, by: str = "id") -> ConcreteTable:
        result: Result = self.execute(
            select(self.schema_class).order_by(asc(by)).limit(1)
        )
        # result: Result = await self.execute(
        #     select(self.schema_class).order_by(asc(by)).limit(1)
        # )

        if not (_result := result.scalar_one_or_none()):
            raise NotFoundError

        return _result

    def _last(self, by: str = "id") -> ConcreteTable:
        result: Result = self.execute(
            select(self.schema_class).order_by(desc(by)).limit(1)
        )
        # result: Result = await self.execute(
        #     select(self.schema_class).order_by(desc(by)).limit(1)
        # )

        if not (_result := result.scalar_one_or_none()):
            raise NotFoundError

        return _result

    def _save(self, payload: dict[str, Any]) -> ConcreteTable:
        try:
            schema = self.schema_class(**payload)
            self._session.add(schema)
            self._session.flush()
            self._session.refresh(schema)
            # await self._session.flush()
            # await self._session.refresh(schema)
            return schema
        except self._ERRORS:
            raise DatabaseError

    def save_all(self, payloads: list[dict[str, Any]]) -> None:
        try:
            instances = [self.schema_class(**payload) for payload in payloads]
            self._session.add_all(instances)
            self._session.flush()
            # await self._session.flush()
        except self._ERRORS:
            raise DatabaseError

    def _all(self) -> Generator[ConcreteTable, None, None]:
        result: Result = self.execute(select(self.schema_class))
        # result: Result = await self.execute(select(self.schema_class))
        schemas = result.scalars().all()

        for schema in schemas:
            yield schema

    def delete(self, id_: int) -> None:
        self.execute(
            delete(self.schema_class).where(
                cast("ColumnElement[bool]", self.schema_class.id == id_)
            )
        )
        # await self.execute(delete(self.schema_class).where(self.schema_class.id == id_))
        self._session.flush()
        # await self._session.flush()

    def commit(self) -> None:
        self._session.commit()

    def rollback(self) -> None:
        self._session.rollback()

    def vacuum(self, has_tablename=False, is_full=False, is_analyze=False) -> None:
        query = "VACUUM"
        if is_full:
            query += " FULL"
        if is_analyze:
            query += " ANALYZE"
        if has_tablename:
            query += " " + self.schema_class.__tablename__
        query += ";"
        if has_tablename:
            # log.debug("vacuum close transaction")
            self._session.execute(text("COMMIT"))
        self._session.execute(text(query))
