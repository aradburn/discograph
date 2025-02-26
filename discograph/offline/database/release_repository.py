import logging
from typing import Generator, Any, List, Sequence

from sqlalchemy import Result, select, update, Select, delete

from discograph import utils
from discograph.exceptions import NotFoundError, DatabaseError
from discograph.offline.database.base_repository import BaseRepository
from discograph.offline.database.release_table import ReleaseTable
from discograph.offline.domain.release import Release

log = logging.getLogger(__name__)


class ReleaseRepository(BaseRepository[ReleaseTable]):
    schema_class = ReleaseTable

    def _get_one_by_query(self, query: Select[tuple[ReleaseTable]]) -> Release:
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError

        release_db = Release.model_validate(instance)
        return release_db.to_domain()

    def _get_all_by_query(self, query: Select[tuple[ReleaseTable]]) -> List[Release]:
        result: Result = self.execute(query)

        instances = result.scalars().all()
        release_dbs = [Release.model_validate(instance) for instance in instances]
        releases = [release_db.to_domain() for release_db in release_dbs]
        return releases

    def all(self) -> Generator[Release, None, None]:
        query = select(ReleaseTable)
        with self._session.execute(
            query, execution_options={"yield_per": 1000}
        ) as results:
            for partition in results.partitions():
                # partition is an iterable that will be at most 1000 items
                for row in partition:
                    yield Release.model_validate(row[0])

    # def all(self) -> Generator[Release, None, None]:
    #     for instance in self._all():
    #         # async for instance in self._all():
    #         yield Release.model_validate(instance)

    def get(self, release_id: int) -> Release:
        query = select(ReleaseTable).where(ReleaseTable.release_id == release_id)

        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError

        return Release.model_validate(instance)

    def create(self, release: Release) -> Release:
        instance: ReleaseTable = self._save(release.model_dump())
        # instance: ReleaseTable = await self._save(schema.model_dump())
        return Release.model_validate(instance)

    def get_ids(self) -> Sequence[int]:
        return self._session.scalars(select(ReleaseTable.release_id)).all()

    def get_batched_ids(self, num_in_batch: int) -> List[List[int]]:
        return utils.batched(self.get_ids(), num_in_batch)

    def update(
        self,
        release_id: int,
        payload: dict[str, Any],
    ) -> ReleaseTable:
        """Updates an existed instance of the model in the related table.
        If some data is not exist in the payload then the null value will
        be passed to the schema class."""

        query = (
            update(self.schema_class)
            .where(ReleaseTable.release_id == release_id)
            .values(payload)
            .returning(self.schema_class)
        )
        result: Result = self._session.execute(query)
        # result: Result = await self.execute(query)
        self._session.flush()
        # await self._session.flush()

        if not (schema := result.scalar_one_or_none()):
            raise DatabaseError

        return schema

    def delete_by_id(self, release_id: int) -> None:
        self.execute(
            delete(self.schema_class).where(ReleaseTable.release_id == release_id)
        )
        # await self.execute(delete(self.schema_class).where(self.schema_class.id == id_))
        # self._session.flush()
        # await self._session.flush()
