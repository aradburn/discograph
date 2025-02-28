import logging
from typing import Any, Generic, Type, Generator, cast

from sqlalchemy import asc, delete, desc, func, select, update, text
from sqlalchemy.engine import Result

__all__ = ("RuntimeBaseRepository",)

from discograph.exceptions import UnprocessableError, DatabaseError, NotFoundError
from discograph.runtime.runtime_database.runtime_base_table import RuntimeConcreteTable
from discograph.runtime.runtime_database.runtime_session import RuntimeSession

log = logging.getLogger(__name__)


class RuntimeBaseRepository(RuntimeSession, Generic[RuntimeConcreteTable]):
    """
    This class implements the base interface for working with the database
    and makes it easier to work with type annotations.

    The Session class implements the database interaction layer.

    Attributes:
        schema_class (Type[RuntimeConcreteTable]): The schema class for the repository.
    """

    schema_class: Type[RuntimeConcreteTable]

    def __init__(self) -> None:
        """
        Initializes the RuntimeBaseRepository instance.

        Raises:
            UnprocessableError: If the schema_class attribute is not set.
        """
        super().__init__()

        if not self.schema_class:
            raise UnprocessableError(
                message="Can not initiate the class without schema_class attribute"
            )

    def _update(
        self, key: str, value: Any, payload: dict[str, Any]
    ) -> RuntimeConcreteTable:
        """
        Updates an existing instance of the model in the related table.
        If some data does not exist in the payload, then the null value will
        be passed to the schema class.

        Args:
            key (str): The key to filter the update.
            value (Any): The value to filter the update.
            payload (dict[str, Any]): The data to update.

        Returns:
            RuntimeConcreteTable: The updated schema instance.

        Raises:
            DatabaseError: If there is an error during the update.
        """
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

    def _get(self, key: str, value: Any) -> RuntimeConcreteTable:
        """
        Returns only one result by filters.

        Args:
            key (str): The key to filter the query.
            value (Any): The value to filter the query.

        Returns:
            RuntimeConcreteTable: The retrieved schema instance.

        Raises:
            NotFoundError: If no result is found.
        """
        query = select(self.schema_class).where(
            cast("ColumnElement[bool]", getattr(self.schema_class, key) == value)
        )
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (_result := result.scalars().one_or_none()):
            raise NotFoundError

        return _result

    def count(self) -> int:
        """
        Counts the number of rows in the table.

        Returns:
            int: The count of rows.

        Raises:
            UnprocessableError: If the count function returns a non-integer value.
        """
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

    def _first(self, by: str = "id") -> RuntimeConcreteTable:
        """
        Returns the first result ordered by the specified column.

        Args:
        Returns:
            RuntimeConcreteTable: The first schema instance.

        Raises:
            NotFoundError: If no result is found.
        """
        result: Result = self.execute(
            select(self.schema_class).order_by(asc(by)).limit(1)
        )
        # result: Result = await self.execute(
        #     select(self.schema_class).order_by(asc(by)).limit(1)
        # )

        if not (_result := result.scalar_one_or_none()):
            raise NotFoundError

        return _result

    def _last(self, by: str = "id") -> RuntimeConcreteTable:
        """
        Returns the last result ordered by the specified column.

        Args:
            by (str): The column to order by. Defaults to "id".

        Returns:
            RuntimeConcreteTable: The last schema instance.

        Raises:
            NotFoundError: If no result is found.
        """
        result: Result = self.execute(
            select(self.schema_class).order_by(desc(by)).limit(1)
        )
        # result: Result = await self.execute(
        #     select(self.schema_class).order_by(desc(by)).limit(1)
        # )

        if not (_result := result.scalar_one_or_none()):
            raise NotFoundError

        return _result

    def _save(self, payload: dict[str, Any]) -> RuntimeConcreteTable:
        """
        Saves a new instance of the model in the related table.

        Args:
            payload (dict[str, Any]): The data to save.

        Returns:
            RuntimeConcreteTable: The saved schema instance.

        Raises:
            DatabaseError: If there is an error during the save.
        """
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
        """
        Saves multiple instances of the model in the related table.

        Args:
            payloads (list[dict[str, Any]]): The data to save.

        Raises:
            DatabaseError: If there is an error during the save.
        """
        try:
            instances = [self.schema_class(**payload) for payload in payloads]
            self._session.add_all(instances)
            self._session.flush()
            # await self._session.flush()
        except self._ERRORS:
            raise DatabaseError

    def _all(self) -> Generator[RuntimeConcreteTable, None, None]:
        """
        Returns all instances of the model in the related table.

        Yields:
            Generator[RuntimeConcreteTable, None, None]: A generator of schema instances.
        """
        result: Result = self.execute(select(self.schema_class))
        # result: Result = await self.execute(select(self.schema_class))
        schemas = result.scalars().all()

        for schema in schemas:
            yield schema

    def delete(self, id_: int) -> None:
        """
        Deletes an instance of the model by its ID.

        Args:
            id_ (int): The ID of the instance to delete.
        """
        self.execute(
            delete(self.schema_class).where(
                cast("ColumnElement[bool]", self.schema_class.id == id_)
            )
        )
        # await self.execute(delete(self.schema_class).where(self.schema_class.id == id_))
        self._session.flush()
        # await self._session.flush()

    def commit(self) -> None:
        """
        Commits the current transaction.
        """
        self._session.commit()

    def rollback(self) -> None:
        """
        Rolls back the current transaction.
        """
        self._session.rollback()

    def vacuum(self, has_tablename=False, is_full=False, is_analyze=False) -> None:
        """
        Performs a VACUUM operation on the database.

        Args:
            has_tablename (bool): Indicates if the table name should be included in the query.
            is_full (bool): Indicates if the FULL option should be used.
            is_analyze (bool): Indicates if the ANALYZE option should be used.
        """
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
