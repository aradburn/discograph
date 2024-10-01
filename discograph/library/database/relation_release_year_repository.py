import logging
from typing import Generator, List

from sqlalchemy import Result, select, Select

from discograph.exceptions import DatabaseError
from discograph.library.database.base_repository import BaseRepository
from discograph.library.database.relation_release_year_table import (
    RelationReleaseYearTable,
)
from discograph.library.domain.relation_release_year import (
    RelationReleaseYearDB,
    RelationReleaseYear,
    RelationReleaseYearUncommitted,
)

log = logging.getLogger(__name__)


class RelationReleaseYearRepository(BaseRepository[RelationReleaseYearTable]):
    schema_class = RelationReleaseYearTable

    @staticmethod
    def _to_domain(
        relation_release_year_db: RelationReleaseYearDB,
    ) -> RelationReleaseYear:
        relation_release_year_db_dict: dict = relation_release_year_db.model_dump()
        return RelationReleaseYear.model_validate(relation_release_year_db_dict)

    # def _get_one_by_query(self, query: Select[tuple[RelationTable]]) -> Relation:
    #     result: Result = self.execute(query)
    #     # result: Result = await self.execute(query)
    #
    #     if not (instance := result.scalars().one_or_none()):
    #         raise NotFoundError
    #
    #     relation_db = RelationDB.model_validate(instance)
    #     return self._to_domain(relation_db)

    def _get_all_by_query(
        self, query: Select[tuple[RelationReleaseYearTable]]
    ) -> List[RelationReleaseYear]:
        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        instances = result.scalars().all()
        relation_dbs = [
            RelationReleaseYearDB.model_validate(instance) for instance in instances
        ]
        return list(map(self._to_domain, relation_dbs))

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
        from discograph.library.database.database_helper import DatabaseHelper

        relation_release_year_dict = relation_release_year.model_dump()
        query = DatabaseHelper.db_helper.generate_insert_query(
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
        return self._to_domain(relation_release_year_db)

    def create_bulk(
        self,
        relation_release_years: List[RelationReleaseYearUncommitted],
        on_conflict_do_nothing=False,
    ) -> None:
        from discograph.library.database.database_helper import DatabaseHelper

        relation_release_year_dicts = []
        for relation_release_year in relation_release_years:
            relation_release_year_dict = relation_release_year.model_dump()
            relation_release_year_dicts.append(relation_release_year_dict)
        query = DatabaseHelper.db_helper.generate_insert_bulk_query(
            self.schema_class, relation_release_year_dicts, on_conflict_do_nothing
        )
        self._session.execute(query)
