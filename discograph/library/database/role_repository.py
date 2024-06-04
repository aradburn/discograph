import logging
from typing import Generator

from sqlalchemy import Result, select

from discograph.exceptions import NotFoundError
from discograph.library.data_access_layer.role_data_access import RoleDataAccess
from discograph.library.database.base_repository import BaseRepository
from discograph.library.database.role_table import RoleTable
from discograph.library.domain.role import Role, RoleUncommited

log = logging.getLogger(__name__)


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
        from discograph.library.cache.cache_manager import cache

        normalized_name = RoleDataAccess.normalize(name)

        role_key_str = f"ROLE-{normalized_name}"
        role = cache.get(role_key_str)
        if role:
            return role

        query = select(RoleTable).where(RoleTable.role_name == normalized_name)

        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError

        role = Role.model_validate(instance)
        cache.set(role_key_str, role)
        # log.debug(f"cached role: {role_key_str}")
        return role

    def create(self, schema: RoleUncommited) -> Role:
        instance: RoleTable = self._save(schema.model_dump())
        # instance: RoleTable = await self._save(schema.model_dump())
        return Role.model_validate(instance)
