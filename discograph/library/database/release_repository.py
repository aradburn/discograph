import logging
from random import random
from typing import Generator, Any, List

from sqlalchemy import Result, select, update, Select

from discograph import utils
from discograph.exceptions import NotFoundError, DatabaseError
from discograph.library.database.base_repository import BaseRepository
from discograph.library.database.release_table import ReleaseTable
from discograph.library.domain.release import Release

log = logging.getLogger(__name__)


class ReleaseRepository(BaseRepository[ReleaseTable]):
    schema_class = ReleaseTable

    @staticmethod
    def _to_domain(release_db: Release) -> Release:
        # print(f"_to_domain")
        return release_db

    def _get_one_by_query(self, query: Select[tuple[ReleaseTable]]) -> Release:
        # print(f"_get_one_by_query")
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError

        # print(f"instance: {instance}")

        release_db = Release.model_validate(instance)
        # print(f"relation_db: {utils.normalize_dict(relation_db)}")
        return self._to_domain(release_db)

    def _get_all_by_query(self, query: Select[tuple[ReleaseTable]]) -> List[Release]:
        # print(f"_get_all_by_query")
        result: Result = self.execute(query)

        instances = result.scalars().all()
        release_dbs = [Release.model_validate(instance) for instance in instances]
        return list(map(self._to_domain, release_dbs))

    def all(self) -> Generator[Release, None, None]:
        for instance in self._all():
            # async for instance in self._all():
            yield Release.model_validate(instance)

    def get(self, release_id: int) -> Release:
        query = (
            select(ReleaseTable)
            # .options(
            #     joinedload(getattr(self.schema_class, "user")),
            #     joinedload(getattr(self.schema_class, "product")),
            # )
            .where(ReleaseTable.release_id == release_id)
        )

        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError

        return Release.model_validate(instance)

    def create(self, release: Release) -> Release:
        instance: ReleaseTable = self._save(release.model_dump())
        # instance: ReleaseTable = await self._save(schema.model_dump())
        return Release.model_validate(instance)

    def get_chunked_release_ids(self, concurrency_multiplier=1):
        from discograph.database import get_concurrency_count

        all_ids = self._session.scalars(
            select(ReleaseTable.release_id).order_by(ReleaseTable.release_id)
        ).all()

        num_chunks = get_concurrency_count() * concurrency_multiplier
        return utils.split_tuple(num_chunks, all_ids)

    def get_random_release(self) -> Release:
        n = random()
        query = (
            select(ReleaseTable)
            .where(ReleaseTable.random > n)
            .order_by(ReleaseTable.random)
            .limit(1)
        )
        t = self._get_one_by_query(query)
        return t

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
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)
        self._session.flush()
        # await self._session.flush()

        if not (schema := result.scalar_one_or_none()):
            raise DatabaseError

        return schema
