import logging
from typing import Generator, List

from sqlalchemy import Result, select, Select

from discograph.exceptions import DatabaseError
from discograph.offline.database.base_repository import BaseRepository
from discograph.offline.database.relation_release_year_table import (
    RelationReleaseYearTable,
)
from discograph.offline.domain.relation_release_year import (
    RelationReleaseYearDB,
    RelationReleaseYear,
    RelationReleaseYearUncommitted,
)

log = logging.getLogger(__name__)


class RelationReleaseYearRepository(BaseRepository[RelationReleaseYearTable]):
    schema_class = RelationReleaseYearTable

    def _get_all_by_query(
        self, query: Select[tuple[RelationReleaseYearTable]]
    ) -> List[RelationReleaseYear]:
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        instances = result.scalars().all()
        relation_release_year_dbs = [
            RelationReleaseYearDB.model_validate(instance) for instance in instances
        ]
        relation_release_years = [
            relation_release_year_db.to_domain()
            for relation_release_year_db in relation_release_year_dbs
        ]
        return relation_release_years

    def all(self) -> Generator[RelationReleaseYear, None, None]:
        for instance in self._all():
            # async for instance in self._all():
            yield RelationReleaseYear.model_validate(instance)

    def get(self, relation_id: int) -> List[RelationReleaseYear]:
        # print(f"get")
        query = (
            select(RelationReleaseYearTable)
            # .options(
            #     joinedload(RelationTable.role),
            # )
            .where(RelationReleaseYearTable.relation_id == relation_id)
        )

        return self._get_all_by_query(query)

    def create(
        self,
        relation_release_year: RelationReleaseYearUncommitted,
        on_conflict_do_nothing=False,
    ) -> RelationReleaseYear:
        from discograph.offline.offline_database_manager import OfflineDatabaseManager

        relation_release_year_dict = relation_release_year.model_dump()
        query = OfflineDatabaseManager.offline_database_helper.generate_insert_query(
            self.schema_class, relation_release_year_dict, on_conflict_do_nothing
        )
        result: Result = self._session.execute(query)
        # result: Result = await self.execute(query)
        self._session.flush()
        # await self._session.flush()

        if not (instance := result.scalar_one_or_none()):
            raise DatabaseError

        relation_release_year_db = RelationReleaseYearDB.model_validate(instance)
        # print(f"relation_db: {utils.normalize_dict(relation_db)}")
        return relation_release_year_db.to_domain()

    def create_bulk(
        self,
        relation_release_years: List[RelationReleaseYearUncommitted],
        on_conflict_do_nothing=False,
    ) -> None:
        from discograph.offline.offline_database_manager import OfflineDatabaseManager

        relation_release_year_dicts = []
        for relation_release_year in relation_release_years:
            relation_release_year_dict = relation_release_year.model_dump()
            relation_release_year_dicts.append(relation_release_year_dict)
        query = (
            OfflineDatabaseManager.offline_database_helper.generate_insert_bulk_query(
                self.schema_class, relation_release_year_dicts, on_conflict_do_nothing
            )
        )
        self._session.execute(query)
