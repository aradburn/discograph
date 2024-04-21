import logging

from sqlalchemy import Result, select

from discograph.exceptions import NotFoundError
from discograph.library.database.base_repository import BaseRepository
from discograph.library.database.release_info_table import ReleaseInfoTable
from discograph.library.domain.release_info import ReleaseInfoUncommitted, ReleaseInfo

log = logging.getLogger(__name__)


class ReleaseInfoRepository(BaseRepository[ReleaseInfoTable]):
    schema_class = ReleaseInfoTable

    def get(self, relation_id: int, release_id: int) -> ReleaseInfo:
        query = select(ReleaseInfoTable).where(
            (ReleaseInfoTable.relation_id == relation_id)
            & (ReleaseInfoTable.release_id == release_id)
        )

        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError

        return ReleaseInfo.model_validate(instance)

    def create(self, schema: ReleaseInfoUncommitted) -> ReleaseInfo:
        instance: ReleaseInfoTable = self._save(schema.model_dump())
        # instance: ReleaseInfoTable = await self._save(schema.model_dump())
        return ReleaseInfo.model_validate(instance)
