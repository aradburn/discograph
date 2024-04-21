import logging
import multiprocessing

from discograph.database import get_concurrency_count
from discograph.exceptions import NotFoundError, DatabaseError
from discograph.library.data_access_layer.entity_data_access import EntityDataAccess
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.database.entity_table import EntityTable
from discograph.library.database.transaction import transaction
from discograph.library.fields.entity_type import EntityType
from discograph.logging_config import LOGGING_TRACE

log = logging.getLogger(__name__)


class WorkerEntityPassTwo(multiprocessing.Process):
    def __init__(self, entity_type: EntityType, indices):
        super().__init__()
        self.entity_type = entity_type
        self.indices = indices

    def run(self):
        proc_name = self.name
        proc_number = proc_name.split("-")[-1]
        corpus = {}

        count = 0
        total_count = len(self.indices)

        if get_concurrency_count() > 1:
            DatabaseHelper.initialize()

        for i, entity_id in enumerate(self.indices):
            max_attempts = 10
            error = True
            while error and max_attempts != 0:
                error = False
                with transaction():
                    entity_repository = EntityRepository()
                    try:
                        self.worker_pass_two_single(
                            entity_repository=entity_repository,
                            entity_id=entity_id,
                            entity_type=self.entity_type,
                            annotation=proc_number,
                            corpus=corpus,
                        )
                        count += 1
                        if count % 10000 == 0:
                            log.info(
                                f"[{proc_name}] processed {count} of {total_count}"
                            )
                    except (DatabaseError, NotFoundError):
                        log.exception(
                            f"ERROR 1: {entity_id}-{self.entity_type} in process: {proc_number}"
                        )
                        # session.rollback()
                        max_attempts -= 1
                        error = True
                    # else:
                    # session.commit()
            if error:
                log.debug(f"Error in updating references for entity: {entity_id}")
        # session.close()

        log.info(f"[{proc_name}] processed {count} of {total_count}")

    # PUBLIC METHODS

    @staticmethod
    def worker_pass_two_single(
        *,
        entity_repository: EntityRepository,
        entity_id: int,
        entity_type: EntityType,
        annotation="",
        corpus=None,
    ):
        # log.debug(f"id: {entity_id}-{entity_type}")
        entity = entity_repository.get(entity_id, entity_type)

        corpus = corpus or {}
        changed = EntityDataAccess.resolve_references_from_entity(
            entity_repository, entity, corpus
        )
        if changed:
            if LOGGING_TRACE:
                log.debug(
                    f"Entity (Pass 2) [{annotation}]\t"
                    + f"          (id: {entity_id}-{entity_type}): {entity.entity_name}"
                )
            entity_repository.update(
                entity_id, entity_type, {EntityTable.entities.key: entity.entities}
            )
            # flag_modified(entity, cls.entities.key)
            entity_repository.commit()

    # @classmethod
    # def set_search_content(cls, session: Session):
    #     for entity in cls.get_entity_iterator(session, entity_type=EntityType.ARTIST):
    #         with session.begin():
    #             entity.search_content = cls.string_to_tsvector(entity.entity_name)
    #             # document.save()
    #             log.debug(
    #                 f"search_content ({entity.entity_type}:{entity.entity_id}):\t"
    #                 + f"{entity.entity_name} -> {entity.search_content}"
    #             )
    #     for entity in cls.get_entity_iterator(session, entity_type=EntityType.LABEL):
    #         with session.begin():
    #             entity.search_content = cls.string_to_tsvector(entity.entity_name)
    #             # document.save()
    #             log.debug(
    #                 f"search_content ({entity.entity_type}:{entity.entity_id}):\t"
    #                 + f"{entity.entity_name} -> {entity.search_content}"
    #             )

    # @staticmethod
    # def resolve_references(
    #     repository: EntityRepository, entity: Entity, corpus: dict
    # ) -> bool:
    #     # log.debug(f"entity: {self}")
    #     if not entity.entities:
    #         return False
    #
    #     changed = False
    #     if entity.entity_type == EntityType.ARTIST:
    #         for section in ("aliases", "groups", "members"):
    #             if section not in entity.entities:
    #                 continue
    #             for entity_name in entity.entities[section].keys():
    #                 key = (entity.entity_type, entity_name)
    #                 WorkerEntityPassTwo.update_corpus(repository, corpus, key)
    #                 if key in corpus:
    #                     entity.entities[section][entity_name] = corpus[key]
    #                     changed = True
    #     elif entity.entity_type == EntityType.LABEL:
    #         for section in ("parent_label", "sublabels"):
    #             if section not in entity.entities:
    #                 continue
    #             for entity_name in entity.entities[section].keys():
    #                 key = (entity.entity_type, entity_name)
    #                 WorkerEntityPassTwo.update_corpus(repository, corpus, key)
    #                 if key in corpus:
    #                     entity.entities[section][entity_name] = corpus[key]
    #                     changed = True
    #     else:
    #         raise ValueError("Bad entity_type")
    #     return changed

    # def roles_to_relation_count(self, roles) -> int:
    #     count = 0
    #     relation_counts = self.relation_counts or {}
    #     for role in roles:
    #         if role == "Alias":
    #             if "aliases" in self.entities:
    #                 count += len(cast(Dict, self.entities["aliases"]))
    #         elif role == "Member Of":
    #             if "groups" in self.entities:
    #                 count += len(cast(Dict, self.entities["groups"]))
    #             if "members" in self.entities:
    #                 count += len(cast(Dict, self.entities["members"]))
    #         elif role == "Sublabel Of":
    #             if "parent_label" in self.entities:
    #                 count += len(cast(Dict, self.entities["parent_label"]))
    #             if "sublabels" in self.entities:
    #                 count += len(cast(Dict, self.entities["sublabels"]))
    #         else:
    #             count += relation_counts.get(role, 0)
    #     return count
    #
    # @classmethod
    # def search_multi(cls, session: Session, entity_keys):
    #     artist_ids: List[int] = []
    #     label_ids: List[int] = []
    #     for entity_id, entity_type in entity_keys:
    #         if entity_type == EntityType.ARTIST:
    #             artist_ids.append(entity_id)
    #         elif entity_type == EntityType.LABEL:
    #             label_ids.append(entity_id)
    #     if artist_ids and label_ids:
    #         where_clause = (
    #             (cls.entity_type == EntityType.ARTIST)
    #             & cast("ColumnElement[bool]", (cls.entity_id.in_(artist_ids)))
    #         ) | (
    #             (cls.entity_type == EntityType.LABEL)
    #             & cast("ColumnElement[bool]", (cls.entity_id.in_(label_ids)))
    #         )
    #     elif artist_ids:
    #         where_clause = (cls.entity_type == EntityType.ARTIST) & (
    #             cast("ColumnElement[bool]", cls.entity_id.in_(artist_ids))
    #         )
    #     else:
    #         where_clause = (cls.entity_type == EntityType.LABEL) & (
    #             cast("ColumnElement[bool]", cls.entity_id.in_(label_ids))
    #         )
    #     # log.debug(f"            search_multi where_clause: {where_clause}")
    #     return session.scalars(select(cls).where(where_clause))
    #
    # def structural_roles_to_entity_keys(self, roles):
    #     entity_keys = set()
    #     if self.entity_type == EntityType.ARTIST:
    #         if "Alias" in roles:
    #             for section in ("aliases",):
    #                 if section not in self.entities:
    #                     continue
    #                 for entity_id in self.entities[section].values():
    #                     if not entity_id:
    #                         continue
    #                     entity_keys.add((self.entity_type, entity_id))
    #         if "Member Of" in roles:
    #             for section in ("groups", "members"):
    #                 if section not in self.entities:
    #                     continue
    #                 for entity_id in self.entities[section].values():
    #                     if not entity_id:
    #                         continue
    #                     entity_keys.add((self.entity_type, entity_id))
    #     elif self.entity_type == EntityType.LABEL:
    #         if "Sublabel Of" in roles:
    #             for section in ("parent_label", "sublabels"):
    #                 if section not in self.entities:
    #                     continue
    #                 for entity_id in self.entities[section].values():
    #                     if not entity_id:
    #                         continue
    #                     entity_keys.add((self.entity_type, entity_id))
    #     return entity_keys
    #
    # def structural_roles_to_relations(self, roles) -> dict[str, RelationDB]:
    #     # log.debug(f"            structural_roles_to_relations entity: {self}")
    #     # log.debug(
    #     #     f"            structural_roles_to_relations entities: {self.entities}"
    #     # )
    #     # log.debug(f"            structural_roles_to_relations roles: {roles}")
    #     relations = {}
    #     if self.entity_type == EntityType.ARTIST:
    #         role = "Alias"
    #         if role in roles and "aliases" in self.entities:
    #             for entity_id in self.entities["aliases"].values():
    #                 if not entity_id:
    #                     continue
    #                 ids = sorted((entity_id, self.entity_id))
    #                 relation = self.create_relation(
    #                     entity_one_id=ids[0],
    #                     entity_one_type=self.entity_type,
    #                     entity_two_id=ids[1],
    #                     entity_two_type=self.entity_type,
    #                     role=role,
    #                 )
    #                 relations[relation.link_key] = relation
    #         role = "Member Of"
    #         if role in roles:
    #             if "groups" in self.entities:
    #                 for entity_id in self.entities["groups"].values():
    #                     if not entity_id:
    #                         continue
    #                     relation = self.create_relation(
    #                         entity_one_id=self.entity_id,
    #                         entity_one_type=self.entity_type,
    #                         entity_two_id=entity_id,
    #                         entity_two_type=self.entity_type,
    #                         role=role,
    #                     )
    #                     relations[relation.link_key] = relation
    #             if "members" in self.entities:
    #                 for entity_id in self.entities["members"].values():
    #                     if not entity_id:
    #                         continue
    #                     relation = self.create_relation(
    #                         entity_one_id=entity_id,
    #                         entity_one_type=self.entity_type,
    #                         entity_two_id=self.entity_id,
    #                         entity_two_type=self.entity_type,
    #                         role=role,
    #                     )
    #                     relations[relation.link_key] = relation
    #     elif self.entity_type == EntityType.LABEL and "Sublabel Of" in roles:
    #         role = "Sublabel Of"
    #         if "parent_label" in self.entities:
    #             for entity_id in self.entities["parent_label"].values():
    #                 if not entity_id:
    #                     continue
    #                 relation = self.create_relation(
    #                     entity_one_id=self.entity_id,
    #                     entity_one_type=self.entity_type,
    #                     entity_two_id=entity_id,
    #                     entity_two_type=self.entity_type,
    #                     role=role,
    #                 )
    #                 relations[relation.link_key] = relation
    #         if "sublabels" in self.entities:
    #             for entity_id in self.entities["sublabels"].values():
    #                 if not entity_id:
    #                     continue
    #                 relation = self.create_relation(
    #                     entity_one_id=entity_id,
    #                     entity_one_type=self.entity_type,
    #                     entity_two_id=self.entity_id,
    #                     entity_two_type=self.entity_type,
    #                     role=role,
    #                 )
    #                 relations[relation.link_key] = relation
    #     # log.debug(f"            structural_roles_to_relations relations: {relations}")
    #     return relations

    # @classmethod
    # def update_corpus(cls, repository: EntityRepository, corpus: Dict, key):
    #     from discograph.library.cache.cache_manager import cache
    #
    #     # log.debug(f"            corpus before: {corpus}")
    #     if key in corpus:
    #         return
    #
    #     entity_type, entity_name = key
    #     key_str = f"{entity_name}-{entity_type}"
    #     entity_id = cache.get(key_str)
    #     if entity_id == "###":
    #         return
    #
    #     # if entity_id is not None:
    #     #     log.debug(f"cache hit for {key_str}")
    #     if entity_id is None:
    #         # log.debug(f"not cached, try db")
    #         try:
    #             entity = repository.get_by_type_and_name(entity_type, entity_name)
    #             entity_id = entity.entity_id
    #             cache.set(key_str, entity_id)
    #             # log.debug(f"cache set for {key_str} -> {entity_id}")
    #
    #             # query = cls.select().where(
    #             #     cls.entity_type == entity_type,
    #             #     cls.entity_name == entity_name,
    #             # )
    #             # entity_id = query.get().entity_id
    #             #
    #             # cache.set(key_str, entity_id)
    #
    #         except NotFoundError:
    #             log.info(f"            update_corpus key not found: {key}")
    #             entity_id = None
    #             cache.set(key_str, "###")
    #
    #     if entity_id is not None:
    #         # log.debug(f"            key: {key} new value: {entity_id}")
    #         corpus[key] = entity_id
    #         # log.debug(f"            update_corpus key: {key} value: {corpus[key]}")
    #     # else:
    #     #     log.debug(f"entity_id is None")
    #     # log.debug(f"            corpus after : {corpus}")

    # @classmethod
    # def create_relation(
    #     cls,
    #     entity_one_id: int,
    #     entity_one_type: EntityType,
    #     entity_two_id: int,
    #     entity_two_type: EntityType,
    #     role: str,
    # ) -> RelationDB:
    #     return RelationDB(
    #         entity_one_id=entity_one_id,
    #         entity_one_type=entity_one_type,
    #         entity_two_id=entity_two_id,
    #         entity_two_type=entity_two_type,
    #         role=role,
    #     )
    #
    # @classmethod
    # def get_by_pk(
    #     cls: Type["EntityDB"],
    #     session: Session,
    #     pk: tuple[int, EntityType],
    # ) -> Type["EntityDB"]:
    #     entity = session.get(cls, pk)
    #     log.debug(f"entity: {entity}")
    #     relation_counts = entity.relation_counts
    #     if relation_counts is not None:
    #         log.debug(f"relation_counts: {relation_counts}")
    #
    #         mapped_relation_counts = {
    #             RoleType.role_id_to_role_lookup[k]: v
    #             for (k, v) in relation_counts.items()
    #         }
    #         log.debug(f"mapped_relation_counts: {mapped_relation_counts}")
    #
    #         entity.relation_counts = mapped_relation_counts
    #     return entity