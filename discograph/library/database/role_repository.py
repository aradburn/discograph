from typing import Generator

from sqlalchemy import Result, select

from discograph.exceptions import NotFoundError
from discograph.library.data_access_layer.role_data_access import RoleDataAccess
from discograph.library.database.base_repository import BaseRepository
from discograph.library.database.role_table import RoleTable
from discograph.library.domain.role import Role, RoleUncommited


class RoleRepository(BaseRepository[RoleTable]):
    schema_class = RoleTable

    def all(self) -> Generator[Role, None, None]:
        for instance in self._all():
            # async for instance in self._all():
            yield Role.model_validate(instance)

    def get(self, role_id: int) -> Role:
        query = select(RoleTable).where(RoleTable.role_id == role_id)

        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError

        return Role.model_validate(instance)

    def get_by_name(self, name: str) -> Role:
        normalized_name = RoleDataAccess.normalize(name)

        query = select(RoleTable).where(RoleTable.role_name == normalized_name)

        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError

        return Role.model_validate(instance)

    def create(self, schema: RoleUncommited) -> Role:
        instance: RoleTable = self._save(schema.model_dump())
        # instance: RoleTable = await self._save(schema.model_dump())
        return Role.model_validate(instance)
