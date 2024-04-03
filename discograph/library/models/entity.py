import gzip
import logging
import multiprocessing
import pprint
import random
import sys
from typing import List, Dict, cast, Type

from deepdiff import DeepDiff
from sqlalchemy import JSON, PrimaryKeyConstraint, String, select
from sqlalchemy.exc import DatabaseError, NoResultFound, DataError
from sqlalchemy.orm import Mapped, mapped_column, scoped_session, Session
from sqlalchemy.orm.attributes import flag_modified

from discograph import utils
from discograph.database import get_concurrency_count
from discograph.library.database.database_helper import Base, DatabaseHelper
from discograph.library.fields.entity_type import EntityType
from discograph.library.fields.int_enum import IntEnum
from discograph.library.loader_base import LoaderBase
from discograph.library.loader_utils import LoaderUtils
from discograph.library.models.relation import Relation
from discograph.logging_config import LOGGING_TRACE
from discograph.utils import normalise_search_content

log = logging.getLogger(__name__)


class Entity(Base, LoaderBase):
    __tablename__ = "entity"

    # COLUMNS

    entity_id: Mapped[int] = mapped_column(primary_key=True)
    entity_type: Mapped[EntityType] = mapped_column(
        IntEnum(EntityType),
        primary_key=True,
    )
    entity_name: Mapped[str] = mapped_column(String, index=True, nullable=False)
    relation_counts: Mapped[dict | list] = mapped_column(type_=JSON, nullable=True)
    entity_metadata: Mapped[dict | list] = mapped_column(type_=JSON, nullable=False)
    entities: Mapped[dict | list] = mapped_column(type_=JSON, nullable=False)
    search_content: Mapped[str] = mapped_column(String, nullable=False)
    random: Mapped[float]

    __table_args__ = (
        PrimaryKeyConstraint(entity_id, entity_type),
        {},
    )

    # CLASS VARIABLES

    class LoaderPassTwoWorker(multiprocessing.Process):
        def __init__(self, model_class, entity_type: EntityType, indices):
            super().__init__()
            self.model_class = model_class
            self.entity_type = entity_type
            self.indices = indices

        def run(self):
            proc_name = self.name
            proc_number = proc_name.split("-")[-1]
            corpus = {}

            count = 0
            total_count = len(self.indices)

            DatabaseHelper.initialize()
            worker_session = scoped_session(DatabaseHelper.session_factory)

            with worker_session() as session:
                for i, entity_id in enumerate(self.indices):
                    max_attempts = 10
                    error = True
                    while error and max_attempts != 0:
                        error = False
                        session.begin()
                        try:
                            self.model_class.loader_pass_two_single(
                                session,
                                entity_type=self.entity_type,
                                entity_id=entity_id,
                                annotation=proc_number,
                                corpus=corpus,
                            )
                            count += 1
                            if count % 10000 == 0:
                                log.info(
                                    f"[{proc_name}] processed {count} of {total_count}"
                                )
                        except DatabaseError:
                            log.exception(
                                "ERROR 1:",
                                self.entity_type,
                                entity_id,
                                proc_number,
                            )
                            session.rollback()
                            max_attempts -= 1
                            error = True
                        else:
                            session.commit()
                    if error:
                        log.debug(
                            f"Error in updating references for entity: {entity_id}"
                        )
            log.info(f"[{proc_name}] processed {count} of {total_count}")

    class LoaderPassThreeWorker(multiprocessing.Process):
        def __init__(self, model_class, entity_type: EntityType, indices):
            super().__init__()
            self.model_class = model_class
            self.entity_type = entity_type
            self.indices = indices

        def run(self):
            proc_name = self.name
            entity_class_name = self.model_class.__qualname__
            entity_module_name = self.model_class.__module__
            relation_class_name = entity_class_name.replace("Entity", "Relation")
            relation_module_name = entity_module_name.replace("entity", "relation")
            relation_class = getattr(
                sys.modules[relation_module_name], relation_class_name
            )

            count = 0
            total_count = len(self.indices)

            DatabaseHelper.initialize()
            worker_session = scoped_session(DatabaseHelper.session_factory)

            with worker_session() as session:
                for i, entity_id in enumerate(self.indices):
                    with session.begin():
                        try:
                            self.model_class.loader_pass_three_single(
                                relation_class,
                                session,
                                entity_type=self.entity_type,
                                entity_id=entity_id,
                            )
                            count += 1
                            if count % 1000 == 0:
                                log.info(
                                    f"[{proc_name}] processed {count} of {total_count}"
                                )
                        except DatabaseError:
                            log.exception(
                                "ERROR:", self.entity_type, entity_id, proc_name
                            )
            log.info(f"[{proc_name}] processed {count} of {total_count}")

    class UpdaterPassOneWorker(multiprocessing.Process):
        def __init__(self, model_class: Type["Entity"], bulk_updates, processed_count):
            super().__init__()
            self.model_class = model_class
            self.bulk_updates = bulk_updates
            self.processed_count = processed_count

        def run(self):
            proc_name = self.name
            updated_count = 0

            DatabaseHelper.initialize()
            worker_session = scoped_session(DatabaseHelper.session_factory)

            with worker_session() as session:
                for i, updated_entity in enumerate(self.bulk_updates):
                    with session.begin():
                        try:
                            if LOGGING_TRACE:
                                log.debug(
                                    f"update: {updated_entity.entity_id}-{updated_entity.entity_type}"
                                )
                            pk = (updated_entity.entity_id, updated_entity.entity_type)
                            db_entity = session.get(self.model_class, pk)

                            is_changed = False

                            # Update name
                            if db_entity.entity_name != updated_entity.entity_name:
                                db_entity.entity_name = updated_entity.entity_name
                                db_entity.search_content = normalise_search_content(
                                    updated_entity.entity_name
                                )
                                is_changed = True

                            # Update metadata
                            differences = DeepDiff(
                                db_entity,
                                updated_entity,
                                include_paths=[
                                    "entity_metadata",
                                ],
                                ignore_numeric_type_changes=True,
                            )
                            diff = pprint.pformat(differences)
                            if diff != "{}":
                                log.debug(f"diff: {diff}")
                                # log.debug(f"db_entity     : {db_entity}")
                                # log.debug(f"updated_entity: {updated_entity}")

                                db_entity.entity_metadata = (
                                    updated_entity.entity_metadata
                                )

                                flag_modified(
                                    db_entity, self.model_class.entity_metadata.key
                                )
                                is_changed = True

                            if is_changed:
                                session.commit()
                                updated_count += 1
                        except DataError as e:
                            log.exception("Error in updater_pass_one", e)
                            raise e

            log.info(
                f"[{proc_name}] processed_count: {self.processed_count}, updated: {updated_count}"
            )

    # PUBLIC METHODS

    @classmethod
    def loader_pass_one(cls, date: str):
        log.debug("entity loader pass one - artist")
        LoaderBase.loader_pass_one_manager(
            model_class=cls,
            date=date,
            xml_tag="artist",
            id_attr=cls.entity_id.name,
            name_attr="name",
            skip_without=["entity_name"],
        )
        log.debug("entity loader pass one - label")
        LoaderBase.loader_pass_one_manager(
            model_class=cls,
            date=date,
            xml_tag="label",
            id_attr=cls.entity_id.name,
            name_attr="name",
            skip_without=["entity_name"],
        )

    @classmethod
    def updater_pass_one(cls, date: str):
        log.debug("entity updater pass one - artist")
        Entity.updater_pass_one_manager(
            model_class=cls,
            date=date,
            xml_tag="artist",
            id_attr=cls.entity_id.name,
            name_attr="name",
            skip_without=["entity_name"],
        )
        log.debug("entity updater pass one - label")
        Entity.updater_pass_one_manager(
            model_class=cls,
            date=date,
            xml_tag="label",
            id_attr=cls.entity_id.name,
            name_attr="name",
            skip_without=["entity_name"],
        )

    @classmethod
    def updater_pass_one_manager(
        cls,
        model_class,
        date: str,
        xml_tag: str,
        id_attr: str,
        name_attr: str,
        skip_without: List[str],
    ):
        # Updater pass one.
        # initial_count = len(model_class)
        processed_count = 0
        xml_path = LoaderUtils.get_xml_path(xml_tag, date)
        log.info(f"Loading update data from {xml_path}")
        with gzip.GzipFile(xml_path, "r") as file_pointer:
            iterator = LoaderUtils.iterparse(file_pointer, xml_tag)
            bulk_updates = []
            workers = []
            for i, element in enumerate(iterator):
                data = None
                try:
                    data = model_class.tags_to_fields(element)
                    if skip_without:
                        if any(not data.get(_) for _ in skip_without):
                            continue
                    if element.get("id"):
                        data[id_attr] = element.get("id")
                    # data["random"] = random.random()
                    # log.debug(f"{data}")
                    updated_entity = model_class(**data)
                    bulk_updates.append(updated_entity)
                    processed_count += 1
                    # log.debug(f"updated_entity: {updated_entity}")
                    if get_concurrency_count() > 1:
                        # Can do multi threading
                        if len(bulk_updates) >= LoaderBase.BULK_UPDATE_BATCH_SIZE:
                            worker = cls.update_bulk(
                                model_class, bulk_updates, processed_count
                            )
                            worker.start()
                            workers.append(worker)
                            bulk_updates.clear()
                        if len(workers) > get_concurrency_count():
                            worker = workers.pop(0)
                            # log.debug(f"wait for worker {len(workers)} in list")
                            worker.join()
                            if worker.exitcode > 0:
                                log.error(
                                    f"worker {worker.name} exitcode: {worker.exitcode}"
                                )
                                # raise Exception("Error in worker process")
                            worker.terminate()
                except DatabaseError as e:
                    log.exception("Error in updater_pass_one", pprint.pformat(data))
                    raise e

            if len(bulk_updates) > 0:
                worker = cls.update_bulk(model_class, bulk_updates, processed_count)
                worker.start()
                workers.append(worker)
                bulk_updates.clear()

            while len(workers) > 0:
                worker = workers.pop(0)
                worker.join()
                if worker.exitcode > 0:
                    log.error(f"worker {worker.name} exitcode: {worker.exitcode}")
                    # raise Exception("Error in worker process")
                worker.terminate()

    @classmethod
    def update_bulk(cls, model_class, bulk_updates, processed_count):
        worker = cls.UpdaterPassOneWorker(model_class, bulk_updates, processed_count)
        return worker

    @classmethod
    def get_entity_iterator(cls, session: Session, entity_type: EntityType):
        entity_ids = session.scalars(
            select(cls.entity_id).where(cls.entity_type == entity_type)
        ).all()
        for entity_id in entity_ids:
            pk = (entity_id, entity_type)
            entity = session.get(cls, pk)
            yield entity

    @classmethod
    def get_chunked_entity_ids(cls, session: Session, entity_type: EntityType):
        from discograph.database import get_concurrency_count

        all_ids = session.scalars(
            select(cls.entity_id)
            .where(cls.entity_type == entity_type)
            .order_by(cls.entity_id)
        ).all()

        num_chunks = get_concurrency_count()
        return utils.split_tuple(num_chunks, all_ids)

    @classmethod
    def loader_pass_two(cls):
        log.debug("entity loader pass two - artist")

        with DatabaseHelper.session_factory() as session:
            with session.begin():
                entity_type: EntityType = EntityType.ARTIST
                chunked_entity_ids = cls.get_chunked_entity_ids(session, entity_type)
                workers = [
                    cls.LoaderPassTwoWorker(cls, entity_type, x)
                    for x in chunked_entity_ids
                ]

                log.debug(
                    f"entity loader pass two - artist start {len(workers)} workers"
                )
                for worker in workers:
                    worker.start()
                for worker in workers:
                    worker.join()
                    if worker.exitcode > 0:
                        log.debug(f"worker {worker.name} exitcode: {worker.exitcode}")
                        # raise Exception("Error in worker process")
                for worker in workers:
                    worker.terminate()

                log.debug("entity loader pass two - label")
                entity_type: EntityType = EntityType.LABEL
                chunked_entity_ids = cls.get_chunked_entity_ids(session, entity_type)

                workers = [
                    cls.LoaderPassTwoWorker(cls, entity_type, x)
                    for x in chunked_entity_ids
                ]
                log.debug(
                    f"entity loader pass two - label start {len(workers)} workers"
                )
                for worker in workers:
                    worker.start()
                for worker in workers:
                    worker.join()
                    if worker.exitcode > 0:
                        log.debug(f"worker {worker.name} exitcode: {worker.exitcode}")
                        # raise Exception("Error in worker process")
                for worker in workers:
                    worker.terminate()
                log.debug("entity loader pass two - done")

    @classmethod
    def loader_pass_three(cls):
        log.debug("entity loader pass three")

        with DatabaseHelper.session_factory() as session:
            with session.begin():
                entity_type: EntityType = EntityType.ARTIST
                chunked_entity_ids = cls.get_chunked_entity_ids(session, entity_type)
                workers = [
                    cls.LoaderPassThreeWorker(cls, entity_type, x)
                    for x in chunked_entity_ids
                ]
                log.debug(
                    f"entity loader pass three - artist start {len(workers)} workers"
                )
                for worker in workers:
                    worker.start()
                for worker in workers:
                    worker.join()
                    if worker.exitcode > 0:
                        log.debug(
                            f"entity loader worker {worker.name} exitcode: {worker.exitcode}"
                        )
                        # raise Exception("Error in worker process")
                for worker in workers:
                    worker.terminate()
                entity_type: EntityType = EntityType.LABEL
                chunked_entity_ids = cls.get_chunked_entity_ids(session, entity_type)
                workers = [
                    cls.LoaderPassThreeWorker(cls, entity_type, _)
                    for _ in chunked_entity_ids
                ]
                log.debug(
                    f"entity loader pass three - label start {len(workers)} workers"
                )
                for worker in workers:
                    worker.start()
                for worker in workers:
                    worker.join()
                    if worker.exitcode > 0:
                        log.debug(
                            f"entity loader worker {worker.name} exitcode: {worker.exitcode}"
                        )
                        # raise Exception("Error in worker process")
                for worker in workers:
                    worker.terminate()

    @classmethod
    def loader_pass_two_single(
        cls,
        session,
        entity_type: EntityType,
        entity_id: int,
        annotation="",
        corpus=None,
    ):
        pk = (entity_id, entity_type)
        entity = session.get(cls, pk)

        corpus = corpus or {}
        changed = entity.resolve_references(session, corpus)
        if changed:
            if LOGGING_TRACE:
                log.debug(
                    f"{cls.__name__.upper()} (Pass 2) [{annotation}]\t"
                    + f"          (id:{pk}): {entity.entity_name}"
                )
            flag_modified(entity, cls.entities.key)
            session.commit()

    @classmethod
    def loader_pass_three_single(
        cls,
        relation_class,
        session: Session,
        entity_type: EntityType,
        entity_id: int,
    ):
        _relation_counts = {}

        # Get all relations for this entity, where the entity is the first part of the relation
        relations = session.scalars(
            select(relation_class).where(
                (relation_class.entity_one_type == entity_type)
                & (relation_class.entity_one_id == entity_id)
            )
        ).all()
        for relation in relations:
            if relation.role not in _relation_counts:
                _relation_counts[relation.role] = set()
            key = (
                relation.entity_one_type,
                relation.entity_one_id,
                relation.entity_two_type,
                relation.entity_two_id,
            )
            _relation_counts[relation.role].add(key)

        # Get all relations for this entity, where the entity is the second part of the relation
        relations = session.scalars(
            select(relation_class).where(
                (relation_class.entity_two_type == entity_type)
                & (relation_class.entity_two_id == entity_id)
            )
        ).all()
        for relation in relations:
            if relation.role not in _relation_counts:
                _relation_counts[relation.role] = set()
            key = (
                relation.entity_one_type,
                relation.entity_one_id,
                relation.entity_two_type,
                relation.entity_two_id,
            )
            _relation_counts[relation.role].add(key)

        for role, keys in _relation_counts.items():
            _relation_counts[role] = len(keys)

        if not _relation_counts:
            return

        # Update the relation counts for this entity
        pk = (entity_id, entity_type)
        entity = session.get(cls, pk)

        entity.relation_counts = _relation_counts
        session.commit()
        if LOGGING_TRACE:
            log.debug(
                f"{cls.__name__.upper()} (Pass 3)\t"
                + f"(id:{(entity.entity_type, entity.entity_id)}) {entity.entity_name}: {len(_relation_counts)}"
            )

    @classmethod
    def get_random_entity(cls, session: Session):
        n = random.random()
        return session.scalars(
            select(cls)
            .where(
                (cls.random > n)
                & (cls.entity_type == EntityType.ARTIST)
                & ~(cls.entities.is_null())
                & ~(cls.relation_counts.is_null())
            )
            .order_by(cls.random)
            .limit(1)
        ).one()

    @classmethod
    def element_to_names(cls, names):
        result = {}
        if names is None or not len(names):
            return result
        for name in names:
            name = name.text
            if not name:
                continue
            result[name] = None
        return result

    @classmethod
    def element_to_names_and_ids(cls, names_and_ids):
        result = {}
        if names_and_ids is None or not len(names_and_ids):
            return result
        for i in range(0, len(names_and_ids), 2):
            discogs_id = int(names_and_ids[i].text)
            name = names_and_ids[i + 1].text
            result[name] = discogs_id
        return result

    @classmethod
    def element_to_parent_label(cls, parent_label):
        result = {}
        if parent_label is None or parent_label.text is None:
            return result
        name = parent_label.text.strip()
        if not name:
            return result
        result[name] = None
        return result

    @classmethod
    def element_to_sublabels(cls, sublabels):
        result = {}
        if sublabels is None or not len(sublabels):
            return result
        for sublabel in sublabels:
            name = sublabel.text
            if name is None:
                continue
            name = name.strip()
            if not name:
                continue
            result[name] = None
        return result

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

    @classmethod
    def from_element(cls, element):
        data = cls.tags_to_fields(element)
        # log.debug(f"from_element: {cls.entity_name} element: {element} data: {data}")
        return cls(**data)

    @classmethod
    def preprocess_data(cls, data, element):
        data["entity_metadata"] = {}
        data["entities"] = {}
        for key in (
            "aliases",
            "groups",
            "members",
            "parent_label",
            "sublabels",
        ):
            if key in data:
                data["entities"][key] = data.pop(key)
        for key in (
            "contact_info",
            "name_variations",
            "profile",
            "real_name",
            "urls",
        ):
            if key in data:
                data["entity_metadata"][key] = data.pop(key)
        if "entity_name" in data and data.get("entity_name"):
            name = data.get("entity_name")
            data["search_content"] = normalise_search_content(name)
        if element.tag == "artist":
            data["entity_type"] = EntityType.ARTIST
        elif element.tag == "label":
            data["entity_type"] = EntityType.LABEL
        return data

    # @classmethod
    # def get_search_content(cls, string):
    #     pass

    def resolve_references(self, session: Session, corpus: dict) -> bool:
        if not self.entities:
            return False

        changed = False
        if self.entity_type == EntityType.ARTIST:
            for section in ("aliases", "groups", "members"):
                if section not in self.entities:
                    continue
                for entity_name in self.entities[section].keys():
                    key = (self.entity_type, entity_name)
                    self.update_corpus(session, corpus, key)
                    if key in corpus:
                        self.entities[section][entity_name] = corpus[key]
                        changed = True
        elif self.entity_type == EntityType.LABEL:
            for section in ("parent_label", "sublabels"):
                if section not in self.entities:
                    continue
                for entity_name in self.entities[section].keys():
                    key = (self.entity_type, entity_name)
                    self.update_corpus(session, corpus, key)
                    if key in corpus:
                        self.entities[section][entity_name] = corpus[key]
                        changed = True
        return changed

    def roles_to_relation_count(self, roles):
        count = 0
        relation_counts = self.relation_counts or {}
        for role in roles:
            if role == "Alias":
                if "aliases" in self.entities:
                    count += len(cast(Dict, self.entities["aliases"]))
            elif role == "Member Of":
                if "groups" in self.entities:
                    count += len(cast(Dict, self.entities["groups"]))
                if "members" in self.entities:
                    count += len(cast(Dict, self.entities["members"]))
            elif role == "Sublabel Of":
                if "parent_label" in self.entities:
                    count += len(cast(Dict, self.entities["parent_label"]))
                if "sublabels" in self.entities:
                    count += len(cast(Dict, self.entities["sublabels"]))
            else:
                count += relation_counts.get(role, 0)
        return count

    @classmethod
    def search_multi(cls, session: Session, entity_keys):
        artist_ids: List[int] = []
        label_ids: List[int] = []
        for entity_id, entity_type in entity_keys:
            if entity_type == EntityType.ARTIST:
                artist_ids.append(entity_id)
            elif entity_type == EntityType.LABEL:
                label_ids.append(entity_id)
        if artist_ids and label_ids:
            where_clause = (
                (cls.entity_type == EntityType.ARTIST)
                & cast("ColumnElement[bool]", (cls.entity_id.in_(artist_ids)))
            ) | (
                (cls.entity_type == EntityType.LABEL)
                & cast("ColumnElement[bool]", (cls.entity_id.in_(label_ids)))
            )
        elif artist_ids:
            where_clause = (cls.entity_type == EntityType.ARTIST) & (
                cast("ColumnElement[bool]", cls.entity_id.in_(artist_ids))
            )
        else:
            where_clause = (cls.entity_type == EntityType.LABEL) & (
                cast("ColumnElement[bool]", cls.entity_id.in_(label_ids))
            )
        # log.debug(f"            search_multi where_clause: {where_clause}")
        return session.scalars(select(cls).where(where_clause))

    def structural_roles_to_entity_keys(self, roles):
        entity_keys = set()
        if self.entity_type == EntityType.ARTIST:
            if "Alias" in roles:
                for section in ("aliases",):
                    if section not in self.entities:
                        continue
                    for entity_id in self.entities[section].values():
                        if not entity_id:
                            continue
                        entity_keys.add((self.entity_type, entity_id))
            if "Member Of" in roles:
                for section in ("groups", "members"):
                    if section not in self.entities:
                        continue
                    for entity_id in self.entities[section].values():
                        if not entity_id:
                            continue
                        entity_keys.add((self.entity_type, entity_id))
        elif self.entity_type == EntityType.LABEL:
            if "Sublabel Of" in roles:
                for section in ("parent_label", "sublabels"):
                    if section not in self.entities:
                        continue
                    for entity_id in self.entities[section].values():
                        if not entity_id:
                            continue
                        entity_keys.add((self.entity_type, entity_id))
        return entity_keys

    def structural_roles_to_relations(self, roles):
        # log.debug(f"            structural_roles_to_relations entity: {self}")
        # log.debug(
        #     f"            structural_roles_to_relations entities: {self.entities}"
        # )
        # log.debug(f"            structural_roles_to_relations roles: {roles}")
        relations = {}
        if self.entity_type == EntityType.ARTIST:
            role = "Alias"
            if role in roles and "aliases" in self.entities:
                for entity_id in self.entities["aliases"].values():
                    if not entity_id:
                        continue
                    ids = sorted((entity_id, self.entity_id))
                    relation = self.create_relation(
                        entity_one_id=ids[0],
                        entity_one_type=self.entity_type,
                        entity_two_id=ids[1],
                        entity_two_type=self.entity_type,
                        role=role,
                    )
                    relations[relation.link_key] = relation
            role = "Member Of"
            if role in roles:
                if "groups" in self.entities:
                    for entity_id in self.entities["groups"].values():
                        if not entity_id:
                            continue
                        relation = self.create_relation(
                            entity_one_id=self.entity_id,
                            entity_one_type=self.entity_type,
                            entity_two_id=entity_id,
                            entity_two_type=self.entity_type,
                            role=role,
                        )
                        relations[relation.link_key] = relation
                if "members" in self.entities:
                    for entity_id in self.entities["members"].values():
                        if not entity_id:
                            continue
                        relation = self.create_relation(
                            entity_one_id=entity_id,
                            entity_one_type=self.entity_type,
                            entity_two_id=self.entity_id,
                            entity_two_type=self.entity_type,
                            role=role,
                        )
                        relations[relation.link_key] = relation
        elif self.entity_type == EntityType.LABEL and "Sublabel Of" in roles:
            role = "Sublabel Of"
            if "parent_label" in self.entities:
                for entity_id in self.entities["parent_label"].values():
                    if not entity_id:
                        continue
                    relation = self.create_relation(
                        entity_one_id=self.entity_id,
                        entity_one_type=self.entity_type,
                        entity_two_id=entity_id,
                        entity_two_type=self.entity_type,
                        role=role,
                    )
                    relations[relation.link_key] = relation
            if "sublabels" in self.entities:
                for entity_id in self.entities["sublabels"].values():
                    if not entity_id:
                        continue
                    relation = self.create_relation(
                        entity_one_id=entity_id,
                        entity_one_type=self.entity_type,
                        entity_two_id=self.entity_id,
                        entity_two_type=self.entity_type,
                        role=role,
                    )
                    relations[relation.link_key] = relation
        # log.debug(f"            structural_roles_to_relations relations: {relations}")
        return relations

    @classmethod
    def update_corpus(cls, session: Session, corpus: Dict, key):
        from discograph.library.cache.cache_manager import cache

        # log.debug(f"            corpus before: {corpus}")
        if key in corpus:
            return

        entity_type, entity_name = key
        # key_str = f"{entity_type}-{entity_name}"
        key_str = f"{entity_name}-{entity_type}"
        entity_id = cache.get(key_str)
        if entity_id == "###":
            return

        # if entity_id is not None:
        #     log.debug(f"cache hit for {key_str}")
        if entity_id is None:
            # log.debug(f"not cached, try db")
            try:
                entity = session.scalars(
                    select(cls).where(
                        cast("ColumnElement[bool]", cls.entity_type == entity_type)
                        & cast("ColumnElement[bool]", cls.entity_name == entity_name)
                    )
                    # .limit(1)
                ).one()
                entity_id = entity.entity_id
                cache.set(key_str, entity_id)
                # log.debug(f"cache set for {key_str} -> {entity_id}")

                # query = cls.select().where(
                #     cls.entity_type == entity_type,
                #     cls.entity_name == entity_name,
                # )
                # entity_id = query.get().entity_id
                #
                # cache.set(key_str, entity_id)

            except NoResultFound:
                log.info(f"            update_corpus key not found: {key}")
                entity_id = None
                cache.set(key_str, "###")

        if entity_id is not None:
            # log.debug(f"            key: {key} new value: {entity_id}")
            corpus[key] = entity_id
            # log.debug(f"            update_corpus key: {key} value: {corpus[key]}")
        # else:
        #     log.debug(f"entity_id is None")
        # log.debug(f"            corpus after : {corpus}")

    @classmethod
    def create_relation(
        cls,
        entity_one_id: int,
        entity_one_type: EntityType,
        entity_two_id: int,
        entity_two_type: EntityType,
        role: str,
    ) -> Relation:
        pass

    # PUBLIC PROPERTIES

    @property
    def entity_key(self):
        return self.entity_id, self.entity_type

    @property
    def json_entity_key(self):
        entity_id, entity_type = self.entity_key
        if entity_type == EntityType.ARTIST:
            return f"artist-{self.entity_id}"
        elif entity_type == EntityType.LABEL:
            return f"label-{self.entity_id}"
        raise ValueError(self.entity_key)

    @property
    def size(self):
        members = []
        if self.entity_type == EntityType.ARTIST:
            if "members" in self.entities:
                members = self.entities["members"]
        elif self.entity_type == EntityType.LABEL:
            if "sublabels" in self.entities:
                members = self.entities["sublabels"]
        return len(members)


Entity._tags_to_fields_mapping = {
    "aliases": ("aliases", Entity.element_to_names),
    "contact_info": ("contact_info", LoaderUtils.element_to_string),
    "groups": ("groups", Entity.element_to_names),
    "id": ("entity_id", LoaderUtils.element_to_integer),
    "members": ("members", Entity.element_to_names_and_ids),
    "name": ("entity_name", LoaderUtils.element_to_string),
    "namevariations": ("name_variations", LoaderUtils.element_to_strings),
    "parentLabel": ("parent_label", Entity.element_to_parent_label),
    "profile": ("profile", LoaderUtils.element_to_string),
    "realname": ("real_name", LoaderUtils.element_to_string),
    "sublabels": ("sublabels", Entity.element_to_sublabels),
    "urls": ("urls", LoaderUtils.element_to_strings),
}
