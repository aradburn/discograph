import multiprocessing
import re
import sys
import traceback

import peewee
from abjad import sequence, Timer

from discograph import helpers
from discograph.library import EntityType
from discograph.library.bootstrapper import Bootstrapper
from discograph.library.discogs_model import DiscogsModel, database_proxy
from discograph.library.models.relation import Relation


class Entity(DiscogsModel):

    # def __format__(self, format_specification=''):
    #     return json.dumps(
    #         model_to_dict(self, exclude=[
    #             Entity.random,
    #             Entity.relation_counts,
    #             Entity.search_content,
    #         ]),
    #         indent=4,
    #         sort_keys=True,
    #         default=str
    #     )

    # CLASS VARIABLES

    _strip_pattern = re.compile(r'(\(\d+\)|[^(\w\s)]+)')

    class BootstrapPassTwoWorker(multiprocessing.Process):

        def __init__(self, model_class, entity_type: EntityType, indices):
            super().__init__()
            self.model_class = model_class
            self.entity_type = entity_type
            self.indices = indices

        def run(self):
            proc_number = self.name.split('-')[-1]
            corpus = {}
            total = len(self.indices)
            from discograph.helpers import bootstrap_database
            if bootstrap_database:
                database_proxy.initialize(bootstrap_database)
            with DiscogsModel.connection_context():
                for i, entity_id in enumerate(self.indices):
                    with DiscogsModel.atomic():
                        progress = float(i) / total
                        try:
                            self.model_class.bootstrap_pass_two_single(
                                entity_type=self.entity_type,
                                entity_id=entity_id,
                                annotation=proc_number,
                                corpus=corpus,
                                progress=progress,
                            )
                        except peewee.PeeweeException:
                            print('ERROR:', self.entity_type, entity_id, proc_number)
                            traceback.print_exc()

    class BootstrapPassThreeWorker(multiprocessing.Process):

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
            relation_class = getattr(sys.modules[relation_module_name], relation_class_name)

            total = len(self.indices)
            from discograph.helpers import bootstrap_database
            if bootstrap_database:
                database_proxy.initialize(bootstrap_database)
            with DiscogsModel.connection_context():
                for i, entity_id in enumerate(self.indices):
                    with DiscogsModel.atomic():
                        progress = float(i) / total
                        try:
                            self.model_class.bootstrap_pass_three_single(
                                relation_class,
                                entity_type=self.entity_type,
                                entity_id=entity_id,
                                annotation=proc_name,
                                progress=progress,
                            )
                        except peewee.PeeweeException:
                            print('ERROR:', self.entity_type, entity_id, proc_name)
                            traceback.print_exc()

    # PEEWEE FIELDS

    # PEEWEE META

    class Meta:
        db_table = 'entities'
        primary_key = peewee.CompositeKey('entity_type', 'entity_id')
        indexes = (
            (('entity_type', 'name'), False),
        )

    # PUBLIC METHODS

    @classmethod
    def bootstrap(cls):
        cls.drop_table(True)
        cls.create_table()
        cls.bootstrap_pass_one()
        cls.bootstrap_pass_two()

    @classmethod
    def bootstrap_pass_one(cls, **kwargs):
        DiscogsModel.bootstrap_pass_one(
            cls,
            'artist',
            id_attr='entity_id',
            name_attr='name',
            skip_without=['name'],
        )
        DiscogsModel.bootstrap_pass_one(
            cls,
            'label',
            id_attr='entity_id',
            name_attr='name',
            skip_without=['name'],
        )

    @classmethod
    def get_entity_iterator(cls, entity_type: EntityType, pessimistic=False):
        if not pessimistic:
            id_query = cls.select(peewee.fn.Max(cls.entity_id))
            id_query = id_query.where(cls.entity_type == entity_type)
            max_id = id_query.scalar()
            for i in range(1, max_id + 1):
                query = cls.select().where(
                    cls.entity_id == i,
                    cls.entity_type == entity_type,
                    )
                if not query.count():
                    continue
                document = query.get()
                yield document
        else:
            id_query = cls.select(cls.entity_id)
            id_query = id_query.where(cls.entity_type == entity_type)
            for entity in id_query:
                entity_id = entity.entity_id
                entity = cls.select().where(
                    cls.entity_id == entity_id,
                    cls.entity_type == entity_type,
                    ).get()
                yield entity

    @classmethod
    def get_indices(cls, entity_type: EntityType, pessimistic=False):
        indices = []
        if not pessimistic:
            maximum_id = cls.select(
                peewee.fn.Max(cls.entity_id)).where(
                    cls.entity_type == entity_type
                    ).scalar()
            step = maximum_id // helpers.get_concurrency_count()
            for start in range(0, maximum_id, step):
                stop = start + step
                indices.append(range(start, stop))
        else:
            query = cls.select(cls.entity_id)
            query = query.where(cls.entity_type == entity_type)
            query = query.order_by(cls.entity_id)
            query = query.tuples()
            all_ids = tuple(_[0] for _ in query)
            ratio = [1] * helpers.get_concurrency_count()
            # ratio = [1] * multiprocessing.cpu_count()
            for chunk in sequence.partition_by_ratio_of_lengths(all_ids, tuple(ratio)):
                indices.append(chunk)
        return indices

    @classmethod
    def bootstrap_pass_two(cls, pessimistic=False, **kwargs):
        print("entity bootstrap pass two - artist")
        entity_type: EntityType = EntityType.ARTIST
        indices = cls.get_indices(entity_type, pessimistic=pessimistic)

        # worker_class = helpers.create_worker(cls.BootstrapPassTwoWorker)
        # workers = [worker_class(cls, entity_type, x) for x in indices]
        workers = [cls.BootstrapPassTwoWorker(cls, entity_type, x) for x in indices]
        print("entity bootstrap pass two - artist start workers")
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
            if worker.exitcode > 0:
                print(f"worker.exitcode: {worker.exitcode}")
                raise Exception("Error in worker process")
        for worker in workers:
            worker.terminate()

        print("entity bootstrap pass two - label")
        entity_type: EntityType = EntityType.LABEL
        indices = cls.get_indices(entity_type, pessimistic=pessimistic)

        workers = [cls.BootstrapPassTwoWorker(cls, entity_type, x) for x in indices]
        print("entity bootstrap pass two - label start workers")
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
            if worker.exitcode > 0:
                print(f"worker.exitcode: {worker.exitcode}")
                raise Exception("Error in worker process")
        for worker in workers:
            worker.terminate()
        print("entity bootstrap pass two - done")

    @classmethod
    def bootstrap_pass_three(cls, pessimistic=False):
        print("entity bootstrap pass three")

        entity_type: EntityType = EntityType.ARTIST
        indices = cls.get_indices(entity_type, pessimistic=pessimistic)
        workers = [cls.BootstrapPassThreeWorker(cls, entity_type, x) for x in indices]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
            if worker.exitcode > 0:
                print(f"worker.exitcode: {worker.exitcode}")
                raise Exception("Error in worker process")
        for worker in workers:
            worker.terminate()
        entity_type: EntityType = EntityType.LABEL
        indices = cls.get_indices(entity_type, pessimistic=pessimistic)
        workers = [cls.BootstrapPassThreeWorker(cls, entity_type, _) for _ in indices]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
            if worker.exitcode > 0:
                print(f"worker.exitcode: {worker.exitcode}")
                raise Exception("Error in worker process")
        for worker in workers:
            worker.terminate()

    @classmethod
    def bootstrap_pass_two_single(cls, entity_type: EntityType, entity_id: int,
                                  annotation='', corpus=None, progress=None):
        skipped_template = u'{} (Pass 2) {:.3%} [{}]\t[SKIPPED] (id:{}) [{:.8f}]: {}'
        changed_template = u'{} (Pass 2) {:.3%} [{}]\t          (id:{}) [{:.8f}]: {}'
        query = cls.select().where(
            cls.entity_id == entity_id,
            cls.entity_type == entity_type,
            )
        if not query.count():
            return
        document = query.get()
        corpus = corpus or {}
        with Timer(verbose=False) as timer:
            changed = document.resolve_references(corpus)
        if not changed:
            message = skipped_template.format(
                cls.__name__.upper(),
                progress,
                annotation,
                (document.entity_type, document.entity_id),
                timer.elapsed_time,
                document.name,
            )
            if Bootstrapper.is_test:
                print(message)
            return
        document.save()
        message = changed_template.format(
            cls.__name__.upper(),
            progress,
            annotation,
            (document.entity_type, document.entity_id),
            timer.elapsed_time,
            document.name,
        )
        if Bootstrapper.is_test:
            print(message)

    @classmethod
    def bootstrap_pass_three_single(cls, relation_class, entity_type: EntityType, entity_id: int, annotation='', progress=None):
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
        where_clause = (
            (relation_class.entity_one_id == entity_id) &
            (relation_class.entity_one_type == entity_type)
            )
        where_clause |= (
            (relation_class.entity_two_id == entity_id) &
            (relation_class.entity_two_type == entity_type)
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
        message_pieces = [
            cls.__name__.upper(),
            progress,
            annotation,
            (document.entity_type, document.entity_id),
            document.name,
            len(_relation_counts),
            ]
        template = u'{} (Pass 3) {:.3%} [{}]\t(id:{}) {}: {}'
        message = template.format(*message_pieces)
        if Bootstrapper.is_test:
            print(message)

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
        template = 'FIXUP ({}:{}): {} -> {}'
        for document in cls.get_entity_iterator(entity_type=EntityType.ARTIST):
            document.search_content = cls.string_to_tsvector(document.name)
            document.save()
            message = template.format(
                document.entity_type,
                document.entity_id,
                document.name,
                document.search_content,
            )
            if Bootstrapper.is_test:
                print(message)
        for document in cls.get_entity_iterator(entity_type=EntityType.LABEL):
            document.search_content = cls.string_to_tsvector(document.name)
            document.save()
            message = template.format(
                document.entity_type,
                document.entity_id,
                document.name,
                document.search_content,
            )
            if Bootstrapper.is_test:
                print(message)

    @classmethod
    def from_element(cls, element):
        data = cls.tags_to_fields(element)
        # noinspection PyArgumentList
        return cls(**data)

    @classmethod
    def preprocess_data(cls, data, element):
        data['metadata'] = {}
        data['entities'] = {}
        for key in (
            'aliases',
            'groups',
            'members',
            'parent_label',
            'sublabels',
        ):
            if key in data:
                data['entities'][key] = data.pop(key)
        for key in (
            'contact_info',
            'name_variations',
            'profile',
            'real_name',
            'urls',
        ):
            if key in data:
                data['metadata'][key] = data.pop(key)
        if 'name' in data and data.get('name'):
            search_content = data.get('name')
            data['search_content'] = cls.string_to_tsvector(search_content)
        if element.tag == 'artist':
            data['entity_type'] = EntityType.ARTIST
        elif element.tag == 'label':
            data['entity_type'] = EntityType.LABEL
        return data

    def resolve_references(self, corpus):
        changed = False
        if not self.entities:
            return changed
        if self.entity_type == EntityType.ARTIST:
            for section in ('aliases', 'groups', 'members'):
                if section not in self.entities:
                    continue
                for entity_name in self.entities[section].keys():
                    key = (self.entity_type, entity_name)
                    self.update_corpus(corpus, key)
                    if key in corpus:
                        self.entities[section][entity_name] = corpus[key]
                        changed = True
        elif self.entity_type == EntityType.LABEL:
            for section in ('parent_label', 'sublabels'):
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
            if role == 'Alias':
                if 'aliases' in self.entities:
                    count += len(self.entities['aliases'])
            elif role == 'Member Of':
                if 'groups' in self.entities:
                    count += len(self.entities['groups'])
                if 'members' in self.entities:
                    count += len(self.entities['members'])
            elif role == 'Sublabel Of':
                if 'parent_label' in self.entities:
                    count += len(self.entities['parent_label'])
                if 'sublabels' in self.entities:
                    count += len(self.entities['sublabels'])
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
                (
                    (cls.entity_type == EntityType.ARTIST) &
                    (cls.entity_id.in_(artist_ids))
                    ) | (
                    (cls.entity_type == EntityType.LABEL) &
                    (cls.entity_id.in_(label_ids))
                    )
                )
        elif artist_ids:
            where_clause = (
                (cls.entity_type == EntityType.ARTIST) &
                (cls.entity_id.in_(artist_ids))
                )
        else:
            where_clause = (
                (cls.entity_type == EntityType.LABEL) &
                (cls.entity_id.in_(label_ids))
                )
        return cls.select().where(where_clause)

    # @classmethod
    # def search_text(cls, search_string):
    #     search_string = search_string.lower()
    #     # Transliterate the unicode string into a plain ASCII string
    #     search_string = unidecode(search_string, "preserve")
    #     search_string = ','.join(search_string.split())
    #     query = Entity.raw("""
    #         SELECT entity_type,
    #             entity_id,
    #             name,
    #             ts_rank_cd(search_content, query, 63) AS rank
    #         FROM entities,
    #             to_tsquery(%s) query
    #         WHERE query @@ search_content
    #         ORDER BY rank DESC
    #         LIMIT 100
    #         """, search_string)
    #     return query

    # @classmethod
    # def string_to_tsvector(cls, string):
    #     string = string.lower()
    #     # Transliterate the unicode string into a plain ASCII string
    #     string = unidecode(string, "preserve")
    #     string = cls._strip_pattern.sub('', string)
    #     tsvector = peewee.fn.to_tsvector(string)
    #     return tsvector

    def structural_roles_to_entity_keys(self, roles):
        entity_keys = set()
        if self.entity_type == EntityType.ARTIST:
            if 'Alias' in roles:
                for section in ('aliases',):
                    if section not in self.entities:
                        continue
                    for entity_id in self.entities[section].values():
                        if not entity_id:
                            continue
                        entity_keys.add((self.entity_type, entity_id))
            if 'Member Of' in roles:
                for section in ('groups', 'members'):
                    if section not in self.entities:
                        continue
                    for entity_id in self.entities[section].values():
                        if not entity_id:
                            continue
                        entity_keys.add((self.entity_type, entity_id))
        elif self.entity_type == EntityType.LABEL:
            if 'Sublabel Of' in roles:
                for section in ('parent_label', 'sublabels'):
                    if section not in self.entities:
                        continue
                    for entity_id in self.entities[section].values():
                        if not entity_id:
                            continue
                        entity_keys.add((self.entity_type, entity_id))
        return entity_keys

    def structural_roles_to_relations(self, roles):
        relations = {}
        if self.entity_type == EntityType.ARTIST:
            role = 'Alias'
            if role in roles and 'aliases' in self.entities:
                for entity_id in self.entities['aliases'].values():
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
            role = 'Member Of'
            if role in roles:
                if 'groups' in self.entities:
                    for entity_id in self.entities['groups'].values():
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
                if 'members' in self.entities:
                    for entity_id in self.entities['members'].values():
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
        elif self.entity_type == EntityType.LABEL and 'Sublabel Of' in roles:
            role = 'Sublabel Of'
            if 'parent_label' in self.entities:
                for entity_id in self.entities['parent_label'].values():
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
            if 'sublabels' in self.entities:
                for entity_id in self.entities['sublabels'].values():
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
        return relations

    @classmethod
    def update_corpus(cls, corpus, key):
        if key in corpus:
            return
        entity_type, entity_name = key
        query = cls.select().where(
            cls.entity_type == entity_type,
            cls.name == entity_name,
            )
        if query.count():
            corpus[key] = query.get().entity_id

    @classmethod
    def create_relation(cls, entity_one_type, entity_one_id, entity_two_type, entity_two_id, role) -> Relation:
        pass

    # PUBLIC PROPERTIES

    @property
    def entity_key(self):
        return self.entity_type, self.entity_id

    @property
    def json_entity_key(self):
        entity_type, entity_id = self.entity_key
        if entity_type == EntityType.ARTIST:
            return 'artist-{}'.format(self.entity_id)
        elif entity_type == EntityType.LABEL:
            return 'label-{}'.format(self.entity_id)
        raise ValueError(self.entity_key)

    @property
    def size(self):
        members = []
        if self.entity_type == EntityType.ARTIST:
            if 'members' in self.entities:
                members = self.entities['members']
        elif self.entity_type == EntityType.LABEL:
            if 'sublabels' in self.entities:
                members = self.entities['sublabels']
        return len(members)


Entity._tags_to_fields_mapping = {
    'aliases': ('aliases', Entity.element_to_names),
    'contact_info': ('contact_info', Bootstrapper.element_to_string),
    'groups': ('groups', Entity.element_to_names),
    'id': ('entity_id', Bootstrapper.element_to_integer),
    'members': ('members', Entity.element_to_names_and_ids),
    'name': ('name', Bootstrapper.element_to_string),
    'namevariations': ('name_variations', Bootstrapper.element_to_strings),
    'parentLabel': ('parent_label', Entity.element_to_parent_label),
    'profile': ('profile', Bootstrapper.element_to_string),
    'realname': ('real_name', Bootstrapper.element_to_string),
    'sublabels': ('sublabels', Entity.element_to_sublabels),
    'urls': ('urls', Bootstrapper.element_to_strings),
}
