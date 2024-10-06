import logging
from typing import Dict, Tuple, cast

from discograph import utils
from discograph.exceptions import NotFoundError
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.domain.entity import Entity
from discograph.library.domain.relation import RelationResult
from discograph.library.domain.release import Release
from discograph.library.fields.entity_type import EntityType
from discograph.logging_config import LOGGING_TRACE

log = logging.getLogger(__name__)


class EntityDataAccess:
    @staticmethod
    def resolve_references_from_entity(
        entity_repository: EntityRepository, entity: Entity, corpus: dict
    ) -> bool:
        # log.debug(f"entity: {self}")
        if not entity.entities:
            return False

        changed = False
        if entity.entity_type == EntityType.ARTIST:
            for section in ("aliases", "groups", "members"):
                if section not in entity.entities:
                    continue
                for entity_name in entity.entities[section].keys():
                    key = (entity.entity_type, entity_name)
                    EntityDataAccess.update_corpus(entity_repository, corpus, key)
                    if key in corpus:
                        entity.entities[section][entity_name] = corpus[key]
                        changed = True
        elif entity.entity_type == EntityType.LABEL:
            for section in ("parent_label", "sublabels"):
                if section not in entity.entities:
                    continue
                for entity_name in entity.entities[section].keys():
                    key = (entity.entity_type, entity_name)
                    EntityDataAccess.update_corpus(entity_repository, corpus, key)
                    if key in corpus:
                        entity.entities[section][entity_name] = corpus[key]
                        changed = True
        else:
            raise ValueError("Bad entity_type")
        return changed

    @staticmethod
    def resolve_references_from_release(
        entity_repository: EntityRepository, release: Release, corpus, spuriously=False
    ):
        changed = False
        spurious_id = 0
        for entry in release.labels:
            name = entry["name"]
            entity_key = (EntityType.LABEL, name)
            if not spuriously:
                # release_class_name = release.__class__.__qualname__
                # release_module_name = release.__class__.__module__
                # entity_class_name = release_class_name.replace("Release", "Entity")
                # entity_module_name = release_module_name.replace("release", "entity")
                # entity_class = getattr(
                #     sys.modules[entity_module_name], entity_class_name
                # )

                EntityDataAccess.update_corpus(entity_repository, corpus, entity_key)
            if entity_key in corpus:
                entry["id"] = corpus[entity_key]
                changed = True
            elif spuriously:
                spurious_id -= 1
                corpus[entity_key] = spurious_id
                entry["id"] = corpus[entity_key]
                changed = True
        return changed

    @staticmethod
    def update_corpus(
        entity_repository: EntityRepository,
        corpus: Dict,
        entity_key: Tuple[EntityType, str],
    ):
        from discograph.library.cache.cache_manager import cache

        # log.debug(f"            corpus before: {corpus}")
        if entity_key in corpus:
            return

        entity_type, entity_name = entity_key
        entity_key_str = f"{entity_name}-{entity_type}"
        entity_id = cache.get(entity_key_str)
        if entity_id == "###":
            return

        # if entity_id is not None:
        #     log.debug(f"cache hit for {key_str}")
        if entity_id is None:
            # log.debug(f"not cached, try db")
            try:
                entity = entity_repository.get_by_type_and_name(
                    entity_type, entity_name
                )
                entity_id = entity.entity_id
                cache.set(entity_key_str, entity_id)
                # log.debug(f"cache set for {key_str} -> {entity_id}")

                # query = cls.select().where(
                #     cls.entity_type == entity_type,
                #     cls.entity_name == entity_name,
                # )
                # entity_id = query.get().entity_id
                #
                # cache.set(key_str, entity_id)

            except NotFoundError:
                if LOGGING_TRACE:
                    log.debug(f"update_corpus key not found: {entity_key}")
                entity_id = None
                cache.set(entity_key_str, "###")

        if entity_id is not None:
            corpus[entity_key] = entity_id
        # else:
        #     log.debug(f"entity_id is None")
        # log.debug(f"            corpus after : {corpus}")

    @staticmethod
    def normalise_search_content(string: str) -> str:
        string = string.lower()
        string = utils.to_ascii(string)
        string = utils.STRIP_PATTERN.sub("", string)
        string = utils.REMOVE_PUNCTUATION.sub("", string)
        return string
        # TODO handle search
        # tsvector = func.to_tsvector("english", string)
        # return tsvector

    @staticmethod
    def roles_to_relation_count(entity: Entity, roles) -> int:
        count = 0
        relation_counts = entity.relation_counts or {}
        for role in roles:
            if role == "Alias":
                if "aliases" in entity.entities:
                    count += len(cast(Dict, entity.entities["aliases"]))
            elif role == "Member Of":
                if "groups" in entity.entities:
                    count += len(cast(Dict, entity.entities["groups"]))
                if "members" in entity.entities:
                    count += len(cast(Dict, entity.entities["members"]))
            elif role == "Sublabel Of":
                if "parent_label" in entity.entities:
                    count += len(cast(Dict, entity.entities["parent_label"]))
                if "sublabels" in entity.entities:
                    count += len(cast(Dict, entity.entities["sublabels"]))
            else:
                count += relation_counts.get(role, 0)
        return count

    @staticmethod
    def structural_roles_to_relations(
        entity: Entity, roles
    ) -> Dict[str, RelationResult]:
        # log.debug(f"            structural_roles_to_relations entity: {self}")
        # log.debug(
        #     f"            structural_roles_to_relations entities: {self.entities}"
        # )
        # log.debug(f"            structural_roles_to_relations roles: {roles}")
        relations: Dict[str, RelationResult] = {}
        if entity.entity_type == EntityType.ARTIST:
            role = "Alias"
            if role in roles and "aliases" in entity.entities:
                for entity_id in entity.entities["aliases"].values():
                    if not entity_id:
                        continue
                    ids = sorted((entity_id, entity.entity_id))
                    relation = RelationResult(
                        relation_id=0,
                        entity_one_id=ids[0],
                        entity_one_type=entity.entity_type,
                        entity_two_id=ids[1],
                        entity_two_type=entity.entity_type,
                        random=0.0,
                        releases=None,
                        role=role,
                        distance=None,
                        pages=None,
                    )
                    relations[relation.link_key] = relation
            role = "Member Of"
            if role in roles:
                if "groups" in entity.entities:
                    for entity_id in entity.entities["groups"].values():
                        if not entity_id:
                            continue
                        relation = RelationResult(
                            relation_id=0,
                            entity_one_id=entity.entity_id,
                            entity_one_type=entity.entity_type,
                            entity_two_id=entity_id,
                            entity_two_type=entity.entity_type,
                            random=0.0,
                            releases=None,
                            role=role,
                            distance=None,
                            pages=None,
                        )
                        relations[relation.link_key] = relation
                if "members" in entity.entities:
                    for entity_id in entity.entities["members"].values():
                        if not entity_id:
                            continue
                        relation = RelationResult(
                            relation_id=0,
                            entity_one_id=entity_id,
                            entity_one_type=entity.entity_type,
                            entity_two_id=entity.entity_id,
                            entity_two_type=entity.entity_type,
                            random=0.0,
                            releases=None,
                            role=role,
                            distance=None,
                            pages=None,
                        )
                        relations[relation.link_key] = relation
        elif entity.entity_type == EntityType.LABEL and "Sublabel Of" in roles:
            role = "Sublabel Of"
            if "parent_label" in entity.entities:
                for entity_id in entity.entities["parent_label"].values():
                    if not entity_id:
                        continue
                    relation = RelationResult(
                        relation_id=0,
                        entity_one_id=entity.entity_id,
                        entity_one_type=entity.entity_type,
                        entity_two_id=entity_id,
                        entity_two_type=entity.entity_type,
                        random=0.0,
                        releases=None,
                        role=role,
                        distance=None,
                        pages=None,
                    )
                    relations[relation.link_key] = relation
            if "sublabels" in entity.entities:
                for entity_id in entity.entities["sublabels"].values():
                    if not entity_id:
                        continue
                    relation = RelationResult(
                        relation_id=0,
                        entity_one_id=entity_id,
                        entity_one_type=entity.entity_type,
                        entity_two_id=entity.entity_id,
                        entity_two_type=entity.entity_type,
                        random=0.0,
                        releases=None,
                        role=role,
                        distance=None,
                        pages=None,
                    )
                    relations[relation.link_key] = relation
        # log.debug(f"            structural_roles_to_relations relations: {relations}")
        return relations
