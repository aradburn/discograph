import logging
import multiprocessing
import time
from random import random

from discograph.database import get_concurrency_count
from discograph.exceptions import NotFoundError, DatabaseError
from discograph.library.data_access_layer.relation_data_access import RelationDataAccess
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.database.relation_repository import RelationRepository
from discograph.library.database.release_info_repository import ReleaseInfoRepository
from discograph.library.database.release_repository import ReleaseRepository
from discograph.library.database.role_repository import RoleRepository
from discograph.library.database.transaction import transaction
from discograph.library.domain.relation import RelationUncommitted
from discograph.library.domain.release import Release
from discograph.library.domain.release_info import ReleaseInfoUncommitted
from discograph.logging_config import LOGGING_TRACE

log = logging.getLogger(__name__)


class WorkerRelationPassOne(multiprocessing.Process):
    corpus = {}

    def __init__(self, release_ids):
        super().__init__()
        self.release_ids = release_ids

    def run(self):
        proc_name = self.name
        # relation_class_name = self.model_class.__qualname__
        # relation_module_name = self.model_class.__module__
        # release_class_name = relation_class_name.replace("RelationDB", "ReleaseDB")
        # release_module_name = relation_module_name.replace("relationdb", "releasedb")
        # release_class = getattr(sys.modules[release_module_name], release_class_name)

        count = 0
        total_count = len(self.release_ids)

        if get_concurrency_count() > 1:
            DatabaseHelper.initialize()

        relation_repository = RelationRepository()
        release_repository = ReleaseRepository()
        release_info_repository = ReleaseInfoRepository()
        role_repository = RoleRepository()

        for release_id in self.release_ids:
            max_attempts = 100
            error = True
            while error and max_attempts != 0:
                error = False
                with transaction():
                    try:
                        self.loader_pass_one_inner(
                            relation_repository=relation_repository,
                            release_repository=release_repository,
                            release_info_repository=release_info_repository,
                            role_repository=role_repository,
                            release_id=release_id,
                            corpus=self.corpus,
                            annotation=proc_name,
                        )
                        count += 1
                        if count % 1000 == 0:
                            log.info(
                                f"[{proc_name}] processed {count} of {total_count}"
                            )
                    except DatabaseError:
                        log.debug(
                            f"Database record locked in updating relation {max_attempts} goes left"
                        )
                        # session.rollback()
                        max_attempts -= 1
                        error = True
                        time.sleep(100 - max_attempts)
                    # else:
                    #     session.commit()
            if error:
                log.debug(f"Error in updating relations for release: {release_id}")
                # except DatabaseError:
                #     log.exception("Error in LoaderPassOneWorker")
        # session.close()
        log.info(f"[{proc_name}] processed {count} of {total_count}")

    # @classmethod
    # def _as_artist_credits(cls, companies):
    #     artists = []
    #     for company in companies:
    #         artist = {
    #             "name": company["name"],
    #             "id": company["id"],
    #             "roles": [{"name": company["entity_type_name"]}],
    #         }
    #         artists.append(artist)
    #     return artists

    # PUBLIC METHODS

    @classmethod
    def loader_pass_one_inner(
        cls,
        *,
        relation_repository: RelationRepository,
        release_repository: ReleaseRepository,
        release_info_repository: ReleaseInfoRepository,
        role_repository: RoleRepository,
        release_id,
        corpus,
        annotation="",
    ):
        try:
            # log.debug(f"loader_pass_one ({annotation}): {release_id}")
            release = release_repository.get(release_id)
        except NotFoundError:
            log.debug(f"            loader_pass_one_inner id not found: {release_id}")
        else:
            relations = RelationDataAccess.from_release(release)
            if LOGGING_TRACE:
                log.debug(
                    f"Relation (Pass 1) [{annotation}]\t"
                    + f"(release_id:{release.release_id})\t[{len(relations)}] {release.title}"
                )
            for relation_dict in relations:
                # log.debug(f"  loader_pass_one ({annotation}): {relation}")
                cls.create_relation(
                    relation_repository=relation_repository,
                    release_info_repository=release_info_repository,
                    relation_dict=relation_dict,
                    release=release,
                )
                # role_name = relation_dict["role"]
                # del relation_dict["role"]
                # try:
                #     role = role_repository.get_by_name(role_name)
                # except NotFoundError:
                #     log.debug(f"{role_name} not found")
                # else:
                #     cls.create_relation(
                #         relation_repository=relation_repository,
                #         release_info_repository=release_info_repository,
                #         relation_dict=relation_dict,
                #         role=role,
                #         release=release,
                #     )
                #     # cls.update_relation(relation_repository, relation, role.role_id)

    @classmethod
    def create_relation(
        cls,
        *,
        relation_repository: RelationRepository,
        release_info_repository: ReleaseInfoRepository,
        relation_dict: dict,
        release: Release,
    ):
        relation_id = None
        try:
            relation_uncommitted = RelationUncommitted(
                entity_one_id=relation_dict["entity_one_id"],
                entity_one_type=relation_dict["entity_one_type"],
                entity_two_id=relation_dict["entity_two_id"],
                entity_two_type=relation_dict["entity_two_type"],
                role_name=relation_dict["role"],
                random=random(),
            )
            # log.debug(f"new relation: {new_instance}")
            relation_id = relation_repository.create_and_get_id(relation_uncommitted)

            # get new relation_id
            relation_repository.commit()
        except DatabaseError:
            relation_repository.rollback()

        if relation_id is None:
            relation_id = relation_repository.get_id_by_key(
                key=dict(
                    entity_one_id=relation_dict["entity_one_id"],
                    entity_one_type=relation_dict["entity_one_type"],
                    entity_two_id=relation_dict["entity_two_id"],
                    entity_two_type=relation_dict["entity_two_type"],
                    role_name=relation_dict["role"],
                )
            )
            # log.debug(
            #     f"relation already inserted, got existing: {relation.relation_id}"
            # )

        try:
            new_release_info = ReleaseInfoUncommitted(
                relation_id=relation_id,
                release_id=release.release_id,
                release_date=release.release_date,
            )
            # log.debug(f"new relation: {new_instance}")
            release_info = release_info_repository.create(new_release_info)
            # log.debug(f"release_info inserted for release: {release.release_id}")
            # get new relation_id
            release_info_repository.commit()
        except DatabaseError:
            log.error(f"Cannot insert release_info for release: {release.release_id}")
            release_info_repository.rollback()

    # @classmethod
    # def create_relation(
    #     cls, relation_repository: RelationRepository, relation: dict, role: Role
    # ):
    #     try:
    #         new_relation = dict(
    #             entity_one_id=relation["entity_one_id"],
    #             entity_one_type=relation["entity_one_type"],
    #             entity_two_id=relation["entity_two_id"],
    #             entity_two_type=relation["entity_two_type"],
    #             releases={},
    #             random=random(),
    #         )
    #         # log.debug(f"new relation: {new_instance}")
    #         relation_repository.create(new_relation, role)
    #
    #         # get new relation_id
    #         relation_repository.commit()
    #     except DatabaseError:
    #         # get existing relation_id
    #         relation_repository.rollback()
    #         pass
    #
    # @classmethod
    # def update_relation(
    #     cls, relation_repository: RelationRepository, relation: dict, role_id: int
    # ):
    #     if "release_id" in relation:
    #         release_id: str = str(relation["release_id"])
    #
    #         key: dict = {
    #             "entity_one_id": relation["entity_one_id"],
    #             "entity_one_type": relation["entity_one_type"],
    #             "entity_two_id": relation["entity_two_id"],
    #             "entity_two_type": relation["entity_two_type"],
    #             "role_id": role_id,
    #         }
    #         existing_relation = relation_repository.get_by_key(key)
    #         # log.debug(f"before update instance: {updated_instance}")
    #
    #         if release_id not in existing_relation.releases:
    #             existing_relation.releases[release_id] = relation.get("year")
    #             relation_repository.update(
    #                 key, {"releases": existing_relation.releases}
    #             )
    #             # log.debug(f"relation updated releases: {existing_instance}")
    #             # flag_modified(existing_instance, cls.releases.key)
    #             relation_repository.commit()

    # @classmethod
    # def get_random_relation(cls, session: Session, roles: list[str] = None):
    #     while True:
    #         n: float = random.random()
    #         where_clause = cls.random > n
    #         if roles:
    #             where_clause &= cls.role.in_(roles)
    #         relation = session.scalars(
    #             select(cls).where(where_clause).order_by(cls.random, cls.role).limit(1)
    #         ).one_or_none()
    #         if relation:
    #             break
    #
    #     log.debug(f"random relation: {relation}")
    #     return relation

    # @classmethod
    # def get_by_pk(
    #     cls: Type["RelationDB"],
    #     session: Session,
    #     pk: tuple[int, EntityType, int, EntityType, str],
    # ) -> RowMapping | None:
    #     role_subquery = (
    #         select(RoleDB.role_id, RoleDB.role_name)
    #         .where(RoleDB.role_name == pk[4])
    #         .subquery()
    #     )
    #     role_alias = aliased(RoleDB, role_subquery, name="role")
    #
    #     query = (
    #         select(
    #             cls.entity_one_id,
    #             cls.entity_one_type,
    #             cls.entity_two_id,
    #             cls.entity_two_type,
    #             cls.releases,
    #             # role_alias,
    #             role_alias.role_name.label("role"),
    #         )
    #         .join(cls.role.of_type(role_alias))
    #         .where(
    #             (cls.entity_one_id == pk[0])
    #             & (cls.entity_one_type == pk[1])
    #             & (cls.entity_two_id == pk[2])
    #             & (cls.entity_two_type == pk[3])
    #             # & (role_alias == pk[4])
    #             & (role_alias.role_name == pk[4])
    #             # & (cls.role == pk[4])
    #         )
    #     )
    #
    #     relation = session.execute(query).mappings().one_or_none()
    #     return relation

    # @classmethod
    # def denormalize(
    #     cls: Type["RelationDB"],
    #     relation: Type["RelationDB"],
    # ) -> dict[str | Column, any]:
    #     denormalized_relation = {
    #         "entity_one_id": relation["entity_one_id"],
    #         "entity_one_type": relation["entity_one_type"],
    #         "entity_two_id": relation["entity_two_id"],
    #         "entity_two_type": relation["entity_two_type"],
    #         "role": RoleType.role_id_to_role_lookup[relation["role_id"]],
    #         "releases": relation["releases"],
    #     }
    #     log.debug(f"denormalized_relation: {denormalized_relation}")
    #     return denormalized_relation
