import logging
import multiprocessing
import re
import sys

import peewee

from discograph import utils
from discograph.library.database.database_loader import DatabaseLoader
from discograph.library.discogs_model import DiscogsModel
from discograph.library.entity_type import EntityType
from discograph.library.enum_field import EnumField
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
            database_proxy.initialize(DatabaseLoader.loader_database)

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

            # if bootstrap_database:
            #     database_proxy.initialize(bootstrap_database)
            database_proxy.initialize(DatabaseLoader.loader_database)

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

    # PEEWEE FIELDS

    entity_id: peewee.IntegerField
    entity_type: EnumField
    name: peewee.TextField
    relation_counts: peewee.Field
    metadata: peewee.Field
    entities: peewee.Field
    search_content: peewee.Field

    # PEEWEE META

    class Meta:
        table_name = "entities"
        primary_key = peewee.CompositeKey("entity_type", "entity_id")
        indexes = ((("entity_type", "name"), False),)

    # PUBLIC METHODS

    # @classmethod
    # def bootstrap(cls):
    #     cls.drop_table(True)
    #     cls.create_table()
    #     cls.bootstrap_pass_one()
    #     cls.bootstrap_pass_two()

    @classmethod
    def loader_pass_one(cls, date: str):
        DiscogsModel.loader_pass_one_manager(
            model_class=cls,
            date=date,
            xml_tag="artist",
            id_attr="entity_id",
            name_attr="name",
            skip_without=["name"],
        )
        DiscogsModel.loader_pass_one_manager(
            model_class=cls,
            date=date,
            xml_tag="label",
            id_attr="entity_id",
            name_attr="name",
            skip_without=["name"],
        )

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
        log.debug("entity bootstrap pass two - artist")
        entity_type: EntityType = EntityType.ARTIST
        indices = cls.get_indices(entity_type)

        workers = [cls.LoaderPassTwoWorker(cls, entity_type, x) for x in indices]
        log.debug(f"entity bootstrap pass two - artist start {len(workers)} workers")
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
            if worker.exitcode > 0:
                log.debug(f"worker {worker.name} exitcode: {worker.exitcode}")
                # raise Exception("Error in worker process")
        for worker in workers:
            worker.terminate()

        log.debug("entity bootstrap pass two - label")
        entity_type: EntityType = EntityType.LABEL
        indices = cls.get_indices(entity_type)

        workers = [cls.LoaderPassTwoWorker(cls, entity_type, x) for x in indices]
        log.debug(f"entity bootstrap pass two - label start {len(workers)} workers")
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
        query = cls.select().where(
            cls.entity_id == entity_id,
            cls.entity_type == entity_type,
        )
        if not query.count():
            return
        document = query.get()
        corpus = corpus or {}
        changed = document.resolve_references(corpus)
        if changed:
            # log.debug(
            #     f"{cls.__name__.upper()} (Pass 2) {progress:.3%} [{annotation}]\t"
            #     + f"          (id:{(document.entity_type, document.entity_id)}): {document.name}"
            # )
            document.save()
        # else:
        #     log.debug(
        #         f"{cls.__name__.upper()} (Pass 2) {progress:.3%} [{annotation}]\t"
        #         + f"[SKIPPED] (id:{(document.entity_type, document.entity_id)}): {document.name}"
        #     )

    @classmethod
    def loader_pass_three_single(
        cls,
        relation_class,
        entity_type: EntityType,
        entity_id: int,
        annotation="",
        progress=None,
    ):
        query = cls.select(
            cls.entity_id,
            cls.entity_type,
            cls.name,
            cls.relation_counts,
        ).where(
            cls.entity_id == entity_id,
            cls.entity_type == entity_type,
        )
        if not query.count():
            return
        document = query.get()
        entity_id = document.entity_id
        where_clause = (relation_class.entity_one_type == entity_type) & (
            relation_class.entity_one_id == entity_id
        )
        where_clause |= (relation_class.entity_two_type == entity_type) & (
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
        document.relation_counts = _relation_counts
        document.save()
        # log.debug(
        #     f"{cls.__name__.upper()} (Pass 3) {progress:.3%} [{annotation}]\t"
        #     + f"(id:{(document.entity_type, document.entity_id)}) {document.name}: {len(_relation_counts)}"
        # )

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
        # log.debug(f"            corpus before: {corpus}")
        if key in corpus:
            return
        entity_type, entity_name = key
        query = cls.select().where(
            cls.entity_type == entity_type,
            cls.name == entity_name,
        )
        if query.count():
            corpus[key] = query.get().entity_id
            # log.debug(f"            update_corpus key: {key} value: {corpus[key]}")
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
