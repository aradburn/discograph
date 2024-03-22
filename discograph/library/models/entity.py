import gzip
import logging
import multiprocessing
import pprint
import random
import re
import sys
from typing import List

import peewee
from deepdiff import DeepDiff

from discograph import utils
from discograph.database import get_concurrency_count
from discograph.library.database.database_worker import DatabaseWorker
from discograph.library.discogs_model import DiscogsModel
from discograph.library.fields.entity_type import EntityType
from discograph.library.fields.enum_field import EnumField
from discograph.library.loader_utils import LoaderUtils
from discograph.library.models.relation import Relation

log = logging.getLogger(__name__)


class Entity(DiscogsModel):
    # CLASS VARIABLES

    _strip_pattern = re.compile(r"(\(\d+\)|[^(\w\s)]+)")

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

            # from discograph.database import bootstrap_database
            from discograph.library.database.database_proxy import database_proxy

            # from discograph.library.database.database_helper import DatabaseHelper
            #
            # print(f"DatabaseHelper.database: {DatabaseHelper.database}")
            # print(
            #     f"DatabaseHelper.database.is_closed: {DatabaseHelper.database.is_closed()}"
            # )
            # DatabaseHelper.database.close()
            # db_helper.bind_models(DatabaseHelper.database)

            # if bootstrap_database:
            #     database_proxy.initialize(bootstrap_database)
            database_proxy.initialize(DatabaseWorker.worker_database)

            with DiscogsModel.connection_context():
                for i, entity_id in enumerate(self.indices):
                    max_attempts = 10
                    error = True
                    while error and max_attempts != 0:
                        error = False
                        try:
                            with DiscogsModel.atomic():
                                progress = float(i) / total_count
                                try:
                                    self.model_class.loader_pass_two_single(
                                        entity_type=self.entity_type,
                                        entity_id=entity_id,
                                        annotation=proc_number,
                                        corpus=corpus,
                                        progress=progress,
                                    )
                                    count += 1
                                    if count % 10000 == 0:
                                        log.info(
                                            f"[{proc_name}] processed {count} of {total_count}"
                                        )
                                except peewee.PeeweeException:
                                    log.exception(
                                        "ERROR 1:",
                                        self.entity_type,
                                        entity_id,
                                        proc_number,
                                    )
                                    max_attempts -= 1
                                    error = True
                        except peewee.PeeweeException:
                            log.exception(
                                "ERROR 2:", self.entity_type, entity_id, proc_number
                            )
                            max_attempts -= 1
                            error = True
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

            from discograph.library.database.database_proxy import database_proxy

            database_proxy.initialize(DatabaseWorker.worker_database)

            with DiscogsModel.connection_context():
                for i, entity_id in enumerate(self.indices):
                    with DiscogsModel.atomic():
                        progress = float(i) / total_count
                        try:
                            self.model_class.loader_pass_three_single(
                                relation_class,
                                entity_type=self.entity_type,
                                entity_id=entity_id,
                                annotation=proc_name,
                                progress=progress,
                            )
                            count += 1
                            if count % 1000 == 0:
                                log.info(
                                    f"[{proc_name}] processed {count} of {total_count}"
                                )
                        except peewee.PeeweeException:
                            log.exception(
                                "ERROR:", self.entity_type, entity_id, proc_name
                            )
            log.info(f"[{proc_name}] processed {count} of {total_count}")

    class UpdaterPassOneWorker(multiprocessing.Process):
        def __init__(self, model_class, bulk_updates, processed_count):
            super().__init__()
            self.model_class = model_class
            self.bulk_updates = bulk_updates
            self.processed_count = processed_count

        def run(self):
            proc_name = self.name
            from discograph.library.database.database_proxy import database_proxy

            database_proxy.initialize(DatabaseWorker.worker_database)

            updated_count = 0
            with DiscogsModel.connection_context():
                for i, updated_entity in enumerate(self.bulk_updates):
                    with DiscogsModel.atomic():
                        try:
                            # log.debug(
                            #     f"update: {data['entity_id']}-{data['entity_type']}"
                            # )
                            pk = (updated_entity.entity_type, updated_entity.entity_id)
                            old_entity = self.model_class.get_by_id(pk)
                            differences = DeepDiff(
                                old_entity,
                                updated_entity,
                                include_paths=[
                                    "name",
                                    "metadata",
                                ],
                                # exclude_paths=[
                                #     "entity_type",
                                #     "entity_id",
                                #     "search_content",
                                #     "relation_counts",
                                #     "random",
                                #     "entities",
                                #     "dirty_fields",
                                #     "_dirty",
                                # ],
                                ignore_numeric_type_changes=True,
                            )
                            diff = pprint.pformat(differences)
                            if diff != "{}":
                                log.debug(f"diff: {diff}")
                                # differences2 = DeepDiff(
                                #     old_entity.entities,
                                #     updated_entity.entities,
                                #     ignore_numeric_type_changes=True,
                                # )
                                # diff2 = pprint.pformat(differences2)
                                # if diff2 != "{}":
                                #     log.debug(f"entities diff: {diff2}")
                                # Update entity apart from the entities field
                                q = self.model_class.update(
                                    {
                                        self.model_class.name: updated_entity.name,
                                        self.model_class.metadata: updated_entity.metadata,
                                    }
                                ).where(
                                    self.model_class.entity_id
                                    == updated_entity.entity_id,
                                    self.model_class.entity_type
                                    == updated_entity.entity_type,
                                )
                                q.execute()  # Execute the query.
                                updated_count += 1
                        except peewee.PeeweeException as e:
                            log.exception("Error in updater_pass_one")
                            raise e

            log.info(
                f"[{proc_name}] processed_count: {self.processed_count}, updated: {updated_count}"
            )

    # PEEWEE FIELDS

    entity_id: peewee.IntegerField
    entity_type: EnumField
    name: peewee.TextField
    relation_counts: peewee.Field
    metadata: peewee.Field
    entities: peewee.Field
    search_content: peewee.Field
    random: peewee.FloatField

    # PEEWEE META

    class Meta:
        table_name = "entity"
        primary_key = peewee.CompositeKey("entity_type", "entity_id")

    # PUBLIC METHODS

    @classmethod
    def loader_pass_one(cls, date: str):
        log.debug("entity loader pass one - artist")
        DiscogsModel.loader_pass_one_manager(
            model_class=cls,
            date=date,
            xml_tag="artist",
            id_attr=cls.entity_id.name,
            name_attr="name",
            skip_without=["name"],
        )
        log.debug("entity loader pass one - label")
        DiscogsModel.loader_pass_one_manager(
            model_class=cls,
            date=date,
            xml_tag="label",
            id_attr=cls.entity_id.name,
            name_attr="name",
            skip_without=["name"],
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
            skip_without=["name"],
        )
        log.debug("entity updater pass one - label")
        Entity.updater_pass_one_manager(
            model_class=cls,
            date=date,
            xml_tag="label",
            id_attr=cls.entity_id.name,
            name_attr="name",
            skip_without=["name"],
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
                    data["random"] = random.random()
                    # log.debug(f"{data}")
                    updated_entity = model_class(model_class, **data)
                    bulk_updates.append(updated_entity)
                    processed_count += 1
                    # log.debug(f"updated_entity: {updated_entity}")
                    if get_concurrency_count() > 1:
                        # Can do multi threading
                        if len(bulk_updates) >= DiscogsModel.BULK_UPDATE_BATCH_SIZE:
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
                    # if inserted_count >= 10:
                    #     break
                    # document = model_class.create(**data)
                    # template = "{} (Pass 1) (idx:{}) (id:{}): {}"
                    # message = template.format(
                    #     model_class.__name__.upper(),
                    #     i,
                    #     getattr(document, id_attr),
                    #     getattr(document, name_attr),
                    # )
                    # log.debug(message)
                except peewee.DataError as e:
                    log.exception("Error in updater_pass_one", pprint.pformat(data))
                    # traceback.print_exc()
                    raise e

            if len(bulk_updates) > 0:
                worker = cls.update_bulk(model_class, bulk_updates, processed_count)
                worker.start()
                workers.append(worker)
                bulk_updates.clear()

            while len(workers) > 0:
                worker = workers.pop(0)
                # log.debug(
                #     f"wait for worker {worker.name} - {len(workers)} left in list"
                # )
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
    def get_entity_iterator(cls, entity_type: EntityType):
        id_query = cls.select(cls.entity_id)
        id_query = id_query.where(cls.entity_type == entity_type)
        for entity in id_query:
            entity_id = entity.entity_id
            entity = (
                cls.select()
                .where(
                    cls.entity_id == entity_id,
                    cls.entity_type == entity_type,
                )
                .get()
            )
            yield entity

    @classmethod
    def get_indices(cls, entity_type: EntityType):
        from discograph.database import get_concurrency_count

        query = cls.select(cls.entity_id)
        query = query.where(cls.entity_type == entity_type)
        query = query.order_by(cls.entity_id)
        query = query.tuples()
        all_ids = tuple(_[0] for _ in query)
        num_chunks = get_concurrency_count()
        return utils.split_tuple(num_chunks, all_ids)

    @classmethod
    def loader_pass_two(cls, **kwargs):
        log.debug("entity loader pass two - artist")
        entity_type: EntityType = EntityType.ARTIST
        indices = cls.get_indices(entity_type)

        workers = [cls.LoaderPassTwoWorker(cls, entity_type, x) for x in indices]
        log.debug(f"entity loader pass two - artist start {len(workers)} workers")
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
        indices = cls.get_indices(entity_type)

        workers = [cls.LoaderPassTwoWorker(cls, entity_type, x) for x in indices]
        log.debug(f"entity loader pass two - label start {len(workers)} workers")
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

        entity_type: EntityType = EntityType.ARTIST
        indices = cls.get_indices(entity_type)
        workers = [cls.LoaderPassThreeWorker(cls, entity_type, x) for x in indices]
        log.debug(f"entity loader pass three - artist start {len(workers)} workers")
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
        indices = cls.get_indices(entity_type)
        workers = [cls.LoaderPassThreeWorker(cls, entity_type, _) for _ in indices]
        log.debug(f"entity loader pass three - label start {len(workers)} workers")
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
        entity_type: EntityType,
        entity_id: int,
        annotation="",
        corpus=None,
        progress=None,
    ):
        pk = (entity_type, entity_id)
        entity = cls.get_by_id(pk)
        corpus = corpus or {}
        changed = entity.resolve_references(corpus)
        if changed:
            # log.debug(
            #     f"{cls.__name__.upper()} (Pass 2) {progress:.3%} [{annotation}]\t"
            #     + f"          (id:{(document.entity_type, document.entity_id)}): {document.name}"
            # )
            entity.save()
        # query = cls.select().where(
        #     cls.entity_id == entity_id,
        #     cls.entity_type == entity_type,
        # )
        # if not query.count():
        #     return
        # document = query.get()
        # corpus = corpus or {}
        # changed = document.resolve_references(corpus)
        # if changed:
        #     # log.debug(
        #     #     f"{cls.__name__.upper()} (Pass 2) {progress:.3%} [{annotation}]\t"
        #     #     + f"          (id:{(document.entity_type, document.entity_id)}): {document.name}"
        #     # )
        #     document.save()
        # # else:
        # #     log.debug(
        # #         f"{cls.__name__.upper()} (Pass 2) {progress:.3%} [{annotation}]\t"
        # #         + f"[SKIPPED] (id:{(document.entity_type, document.entity_id)}): {document.name}"
        # #     )

    @classmethod
    def loader_pass_three_single(
        cls,
        relation_class,
        entity_type: EntityType,
        entity_id: int,
        annotation="",
        progress=None,
    ):
        _relation_counts = {}

        where_clause = (relation_class.entity_one_type == entity_type) & (
            relation_class.entity_one_id == entity_id
        )
        query = relation_class.select().where(where_clause)
        for relation in query:
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

        where_clause = (relation_class.entity_two_type == entity_type) & (
            relation_class.entity_two_id == entity_id
        )
        query = relation_class.select().where(where_clause)
        _relation_counts = {}
        for relation in query:
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

        try:
            query = cls.select(
                cls.entity_id,
                cls.entity_type,
                cls.name,
                cls.relation_counts,
            ).where(
                cls.entity_id == entity_id,
                cls.entity_type == entity_type,
            )
            # if not query.count():
            #     return
            document = query.get()
            document.relation_counts = _relation_counts
            document.save()
            # log.debug(
            #     f"{cls.__name__.upper()} (Pass 3) {progress:.3%} [{annotation}]\t"
            #     + f"(id:{(document.entity_type, document.entity_id)}) {document.name}: {len(_relation_counts)}"
            # )
        except peewee.DoesNotExist:
            log.debug(f"loader_pass_three_single {entity_id} does not exist")

    # @classmethod
    # def loader_pass_three_single(
    #     cls,
    #     relation_class,
    #     entity_type: EntityType,
    #     entity_id: int,
    #     annotation="",
    #     progress=None,
    # ):
    #     try:
    #         query = cls.select(
    #             cls.entity_id,
    #             cls.entity_type,
    #             cls.name,
    #             cls.relation_counts,
    #         ).where(
    #             cls.entity_id == entity_id,
    #             cls.entity_type == entity_type,
    #         )
    #         # if not query.count():
    #         #     return
    #         document = query.get()
    #     except peewee.DoesNotExist:
    #         log.debug(f"loader_pass_three_single {entity_id} does not exist")
    #     else:
    #         entity_id = document.entity_id
    #         where_clause = (relation_class.entity_one_type == entity_type) & (
    #             relation_class.entity_one_id == entity_id
    #         )
    #         where_clause |= (relation_class.entity_two_type == entity_type) & (
    #             relation_class.entity_two_id == entity_id
    #         )
    #         query = relation_class.select().where(where_clause)
    #         _relation_counts = {}
    #         for relation in query:
    #             if relation.role not in _relation_counts:
    #                 _relation_counts[relation.role] = set()
    #             key = (
    #                 relation.entity_one_type,
    #                 relation.entity_one_id,
    #                 relation.entity_two_type,
    #                 relation.entity_two_id,
    #             )
    #             _relation_counts[relation.role].add(key)
    #         for role, keys in _relation_counts.items():
    #             _relation_counts[role] = len(keys)
    #         if not _relation_counts:
    #             return
    #         document.relation_counts = _relation_counts
    #         document.save()
    #         # log.debug(
    #         #     f"{cls.__name__.upper()} (Pass 3) {progress:.3%} [{annotation}]\t"
    #         #     + f"(id:{(document.entity_type, document.entity_id)}) {document.name}: {len(_relation_counts)}"
    #         # )

    @classmethod
    def get_random(cls):
        n = random.random()
        return cls.select().where(cls.random > n).order_by(cls.random).get()

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

    @classmethod
    def fixup_search_content(cls):
        for document in cls.get_entity_iterator(entity_type=EntityType.ARTIST):
            document.search_content = cls.string_to_tsvector(document.name)
            document.save()
            log.debug(
                f"FIXUP ({document.entity_type}:{document.entity_id}): {document.name} -> {document.search_content}"
            )
        for document in cls.get_entity_iterator(entity_type=EntityType.LABEL):
            document.search_content = cls.string_to_tsvector(document.name)
            document.save()
            log.debug(
                f"FIXUP ({document.entity_type}:{document.entity_id}): {document.name} -> {document.search_content}"
            )

    @classmethod
    def from_element(cls, element):
        data = cls.tags_to_fields(element)
        # log.debug(f"from_element: {cls.name} element: {element} data: {data}")
        # noinspection PyArgumentList
        return cls(**data)

    @classmethod
    def preprocess_data(cls, data, element):
        data["metadata"] = {}
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
                data["metadata"][key] = data.pop(key)
        if "name" in data and data.get("name"):
            search_content = data.get("name")
            # TODO fix string search
            data["search_content"] = cls.string_to_tsvector(search_content)
        if element.tag == "artist":
            data["entity_type"] = EntityType.ARTIST
        elif element.tag == "label":
            data["entity_type"] = EntityType.LABEL
        return data

    @classmethod
    def string_to_tsvector(cls, string):
        pass

    def resolve_references(self, corpus):
        changed = False
        if not self.entities:
            return changed
        if self.entity_type == EntityType.ARTIST:
            for section in ("aliases", "groups", "members"):
                if section not in self.entities:
                    continue
                for entity_name in self.entities[section].keys():
                    key = (self.entity_type, entity_name)
                    self.update_corpus(corpus, key)
                    if key in corpus:
                        self.entities[section][entity_name] = corpus[key]
                        changed = True
        elif self.entity_type == EntityType.LABEL:
            for section in ("parent_label", "sublabels"):
                if section not in self.entities:
                    continue
                for entity_name in self.entities[section].keys():
                    key = (self.entity_type, entity_name)
                    self.update_corpus(corpus, key)
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
                    count += len(self.entities["aliases"])
            elif role == "Member Of":
                if "groups" in self.entities:
                    count += len(self.entities["groups"])
                if "members" in self.entities:
                    count += len(self.entities["members"])
            elif role == "Sublabel Of":
                if "parent_label" in self.entities:
                    count += len(self.entities["parent_label"])
                if "sublabels" in self.entities:
                    count += len(self.entities["sublabels"])
            else:
                count += relation_counts.get(role, 0)
        return count

    @classmethod
    def search_multi(cls, entity_keys):
        artist_ids, label_ids = [], []
        for entity_type, entity_id in entity_keys:
            if entity_type == EntityType.ARTIST:
                artist_ids.append(entity_id)
            elif entity_type == EntityType.LABEL:
                label_ids.append(entity_id)
        if artist_ids and label_ids:
            where_clause = (
                (cls.entity_type == EntityType.ARTIST) & (cls.entity_id.in_(artist_ids))
            ) | ((cls.entity_type == EntityType.LABEL) & (cls.entity_id.in_(label_ids)))
        elif artist_ids:
            where_clause = (cls.entity_type == EntityType.ARTIST) & (
                cls.entity_id.in_(artist_ids)
            )
        else:
            where_clause = (cls.entity_type == EntityType.LABEL) & (
                cls.entity_id.in_(label_ids)
            )
        # log.debug(f"            search_multi where_clause: {where_clause}")
        return cls.select().where(where_clause)

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
                        entity_one_type=self.entity_type,
                        entity_one_id=ids[0],
                        entity_two_type=self.entity_type,
                        entity_two_id=ids[1],
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
                            entity_one_type=self.entity_type,
                            entity_one_id=self.entity_id,
                            entity_two_type=self.entity_type,
                            entity_two_id=entity_id,
                            role=role,
                        )
                        relations[relation.link_key] = relation
                if "members" in self.entities:
                    for entity_id in self.entities["members"].values():
                        if not entity_id:
                            continue
                        relation = self.create_relation(
                            entity_one_type=self.entity_type,
                            entity_one_id=entity_id,
                            entity_two_type=self.entity_type,
                            entity_two_id=self.entity_id,
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
                        entity_one_type=self.entity_type,
                        entity_one_id=self.entity_id,
                        entity_two_type=self.entity_type,
                        entity_two_id=entity_id,
                        role=role,
                    )
                    relations[relation.link_key] = relation
            if "sublabels" in self.entities:
                for entity_id in self.entities["sublabels"].values():
                    if not entity_id:
                        continue
                    relation = self.create_relation(
                        entity_one_type=self.entity_type,
                        entity_one_id=entity_id,
                        entity_two_type=self.entity_type,
                        entity_two_id=self.entity_id,
                        role=role,
                    )
                    relations[relation.link_key] = relation
        # log.debug(f"            structural_roles_to_relations relations: {relations}")
        return relations

    @classmethod
    def update_corpus(cls, corpus, key):
        from discograph.library.cache.cache_manager import cache

        # log.debug(f"            corpus before: {corpus}")
        if key in corpus:
            return

        entity_type, entity_name = key
        # key_str = f"{entity_type}-{entity_name}"
        key_str = f"{entity_name}-{entity_type}"
        entity_id = cache.get(key_str)
        # if entity_id is not None:
        #     log.debug(f"cache hit for {key_str}")
        if entity_id is None:
            # log.debug(f"not cached, try db")
            try:
                query = cls.select(cls.entity_id).where(
                    cls.entity_type == entity_type,
                    cls.name == entity_name,
                )

                document = query.get()
                entity_id = document.entity_id
                cache.set(key_str, entity_id)

                # query = cls.select().where(
                #     cls.entity_type == entity_type,
                #     cls.name == entity_name,
                # )
                # entity_id = query.get().entity_id
                #
                # cache.set(key_str, entity_id)

            except peewee.DoesNotExist:
                # log.info(f"            update_corpus key not found: {key}")
                entity_id = None
                cache.set(key_str, entity_id)
                pass
        if entity_id is not None:
            # log.debug(f"            key: {key} new value: {entity_id}")
            corpus[key] = entity_id
            # log.debug(f"            update_corpus key: {key} value: {corpus[key]}")
        # else:
        #     log.debug(f"entity_id is None")
        # log.debug(f"            corpus after : {corpus}")

    @classmethod
    def create_relation(
        cls, entity_one_type, entity_one_id, entity_two_type, entity_two_id, role
    ) -> Relation:
        pass

    # PUBLIC PROPERTIES

    @property
    def entity_key(self):
        return self.entity_type, self.entity_id

    @property
    def json_entity_key(self):
        entity_type, entity_id = self.entity_key
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
    "name": ("name", LoaderUtils.element_to_string),
    "namevariations": ("name_variations", LoaderUtils.element_to_strings),
    "parentLabel": ("parent_label", Entity.element_to_parent_label),
    "profile": ("profile", LoaderUtils.element_to_string),
    "realname": ("real_name", LoaderUtils.element_to_string),
    "sublabels": ("sublabels", Entity.element_to_sublabels),
    "urls": ("urls", LoaderUtils.element_to_strings),
}
