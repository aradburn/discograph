import logging
from typing import Generator

from sqlalchemy import Result, select

from discograph.exceptions import NotFoundError
from discograph.library.cache.cache_manager import CacheManager
from discograph.runtime.runtime_database import RuntimeRoleTable
from discograph.runtime.runtime_database.runtime_base_repository import (
    RuntimeBaseRepository,
)
from discograph.runtime.runtime_domain.role import RuntimeRole

log = logging.getLogger(__name__)


class RuntimeRoleRepository(RuntimeBaseRepository[RuntimeRoleTable]):
    schema_class = RuntimeRoleTable

    def all(self) -> Generator[RuntimeRole, None, None]:
        for instance in self._all():
            # async for instance in self._all():
            yield RuntimeRole.model_validate(instance)

    def get(self, role_id: int) -> RuntimeRole:
        query = select(RuntimeRoleTable).where(RuntimeRoleTable.id == role_id)

        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError

        return RuntimeRole.model_validate(instance)

    def get_by_name(self, name: str) -> RuntimeRole:
        cache = CacheManager.get_cache()

        role_key_str = f"ROLE-{name}"
        role = cache.get(role_key_str)
        if role:
            return role

        query = select(RuntimeRoleTable).where(RuntimeRoleTable.role_name == name)

        result: Result = self.execute(query)
        # result: Result = await self.execute(query)

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError

        role = RuntimeRole.model_validate(instance)
        cache.set(role_key_str, role)
        # log.debug(f"cached role: {role_key_str}")
        return role

    def create(self, runtime_role: RuntimeRole) -> RuntimeRole:
        instance: RuntimeRoleTable = self._save(runtime_role.model_dump())
        # instance: RoleTable = await self._save(schema.model_dump())
        return RuntimeRole.model_validate(instance)
