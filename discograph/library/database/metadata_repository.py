import logging
from typing import Any

from sqlalchemy import Result, select, update

from discograph.exceptions import NotFoundError, DatabaseError
from discograph.library.database.base_repository import BaseRepository
from discograph.library.database.metadata_table import MetadataTable
from discograph.library.domain.metadata import Metadata
from discograph.library.domain.release import Release

log = logging.getLogger(__name__)


class MetadataRepository(BaseRepository[MetadataTable]):
    schema_class = MetadataTable

    @staticmethod
    def _to_domain(metadata_db: Metadata) -> Metadata:
        # print(f"_to_domain")
        return metadata_db

    def get(self) -> Metadata:
        query = select(MetadataTable)

        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError

        return Metadata.model_validate(instance)

    def create(self, metadata: Metadata) -> Metadata:
        instance: MetadataTable = self._save(metadata.model_dump())
        # instance: MetadataTable = await self._save(metadata.model_dump())
        return Metadata.model_validate(instance)

    def update(
        self,
        payload: dict[str, Any],
    ) -> MetadataTable:
        """Updates an existed instance of the model in the related table.
        If some data is not exist in the payload then the null value will
        be passed to the schema class."""

        query = update(self.schema_class).values(payload).returning(self.schema_class)
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)
        self._session.flush()
        # await self._session.flush()

        if not (schema := result.scalar_one_or_none()):
            raise DatabaseError

        return schema
