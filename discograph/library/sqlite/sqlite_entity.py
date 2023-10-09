import json
import multiprocessing
import re
import threading
import traceback

import peewee
from abjad import sequence, Timer
from playhouse import sqlite_ext
from playhouse.shortcuts import model_to_dict
from unidecode import unidecode

from discograph.library import EntityType
from discograph.library.EnumField import EnumField
from discograph.library.bootstrapper import Bootstrapper
from discograph.library.discogs_model import DiscogsModel
from discograph.library.sqlite.sqlite_relation import SqliteRelation


class SqliteEntity(DiscogsModel):

    def __format__(self, format_specification=''):
        return json.dumps(
            model_to_dict(self, exclude=[
                SqliteEntity.random,
                SqliteEntity.relation_counts,
                SqliteEntity.search_content,
            ]),
            indent=4,
            sort_keys=True,
            default=str
        )

    # CLASS VARIABLES

    _strip_pattern = re.compile(r'(\(\d+\)|[^(\w\s)]+)')

    class BootstrapPassTwoWorker(threading.Thread):

        def __init__(self, entity_type: EntityType, indices):
            threading.Thread.__init__(self)
            self.entity_type = entity_type
            self.indices = indices

        def run(self):
            proc_number = self.name.split('-')[-1]
            corpus = {}
            total = len(self.indices)
            for i, entity_id in enumerate(self.indices):
                with DiscogsModel.connection_context():
                    progress = float(i) / total
                    try:
                        SqliteEntity.bootstrap_pass_two_single(
                            entity_type=self.entity_type,
                            entity_id=entity_id,
                            annotation=proc_number,
                            corpus=corpus,
                            progress=progress,
                            )
                    except peewee.PeeweeException as e:
                        print(
                            'ERROR:',
                            self.entity_type,
                            entity_id,
                            proc_number,
                            )
                        traceback.print_exc()

    class BootstrapPassThreeWorker(threading.Thread):

        def __init__(self, entity_type: EntityType, indices):
            threading.Thread.__init__(self)
            self.entity_type = entity_type
            self.indices = indices

        def run(self):
            proc_name = self.name
            total = len(self.indices)
            for i, entity_id in enumerate(self.indices):
                with DiscogsModel.connection_context():
                    progress = float(i) / total
                    try:
                        SqliteEntity.bootstrap_pass_three_single(
                            entity_type=self.entity_type,
                            entity_id=entity_id,
                            annotation=proc_name,
                            progress=progress,
                            )
                    except peewee.PeeweeException as e:
                        print('ERROR:', self.entity_type, entity_id, proc_name)
                        traceback.print_exc()

    # PEEWEE FIELDS

    entity_id = peewee.IntegerField(index=False)
    entity_type = EnumField(index=False, choices=EntityType)
    name = peewee.TextField(index=True)
    relation_counts = sqlite_ext.JSONField(null=True, index=False)
    metadata = sqlite_ext.JSONField(null=True, index=False)
    entities = sqlite_ext.JSONField(null=True, index=False)
    search_content = sqlite_ext.SearchField()

    # PEEWEE META

    class Meta:
        db_table = 'entities'
        primary_key = peewee.CompositeKey('entity_type', 'entity_id')

    # PUBLIC METHODS

    @classmethod
    def bootstrap(cls):
        print("entity drop_table")
        cls.drop_table(True)
        print("entity create_table")
        cls.create_table()
        cls.bootstrap_pass_one()
        cls.bootstrap_pass_two()

    @classmethod
    def bootstrap_pass_one(cls, **kwargs):
        print("entity bootstrap_pass_one")
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
            step = maximum_id // multiprocessing.cpu_count()
            for start in range(0, maximum_id, step):
                stop = start + step
                indices.append(range(start, stop))
        else:
            query = cls.select(cls.entity_id)
            query = query.where(cls.entity_type == entity_type)
            query = query.order_by(cls.entity_id)
            query = query.tuples()
            all_ids = tuple(_[0] for _ in query)
            ratio = [1] * multiprocessing.cpu_count()
            for chunk in sequence.partition_by_ratio_of_lengths(all_ids, tuple(ratio)):
                indices.append(chunk)
        return indices

    @classmethod
    def bootstrap_pass_two(cls, pessimistic=False, **kwargs):
        print("entity bootstrap_pass_two")
        entity_type: EntityType = EntityType.ARTIST
        indices = cls.get_indices(entity_type, pessimistic=pessimistic)
        workers = [cls.BootstrapPassTwoWorker(entity_type, x) for x in indices]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
        # for worker in workers:
        #     worker.terminate()
        entity_type: EntityType = EntityType.LABEL
        indices = cls.get_indices(entity_type, pessimistic=pessimistic)
        workers = [cls.BootstrapPassTwoWorker(entity_type, x) for x in indices]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
        # for worker in workers:
        #     worker.terminate()

    @classmethod
    def bootstrap_pass_three(cls, pessimistic=False):
        entity_type: EntityType = EntityType.ARTIST
        indices = cls.get_indices(entity_type, pessimistic=pessimistic)
        workers = [cls.BootstrapPassThreeWorker(entity_type, x) for x in indices]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
        # for worker in workers:
        #     worker.terminate()
        entity_type: EntityType = EntityType.LABEL
        indices = cls.get_indices(entity_type, pessimistic=pessimistic)
        workers = [cls.BootstrapPassThreeWorker(entity_type, _) for _ in indices]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
        # for worker in workers:
        #     worker.terminate()

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
            print(message)
            return
        message = changed_template.format(
            cls.__name__.upper(),
            progress,
            annotation,
            (document.entity_type, document.entity_id),
            timer.elapsed_time,
            document.name,
            )
        print(message)
        document.save()

    @classmethod
    def bootstrap_pass_three_single(cls, entity_type: EntityType, entity_id: int, annotation='', progress=None):
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
            (SqliteRelation.entity_one_id == entity_id) &
            (SqliteRelation.entity_one_type == entity_type)
            )
        where_clause |= (
            (SqliteRelation.entity_two_id == entity_id) &
            (SqliteRelation.entity_two_type == entity_type)
            )
        query = SqliteRelation.select().where(where_clause)
        relation_counts = {}
        for relation in query:
            if relation.role not in relation_counts:
                relation_counts[relation.role] = set()
            key = (
                relation.entity_one_type,
                relation.entity_one_id,
                relation.entity_two_type,
                relation.entity_two_id,
                )
            relation_counts[relation.role].add(key)
        for role, keys in relation_counts.items():
            relation_counts[role] = len(keys)
        if not relation_counts:
            return
        document.relation_counts = relation_counts
        document.save()
        message_pieces = [
            cls.__name__.upper(),
            progress,
            annotation,
            (document.entity_type, document.entity_id),
            document.name,
            len(relation_counts),
            ]
        template = u'{} (Pass 3) {:.3%} [{}]\t(id:{}) {}: {}'
        message = template.format(*message_pieces)

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
        for document in cls.get_entity_iterator(entity_type=EntityType.LABEL):
            document.search_content = cls.string_to_tsvector(document.name)
            document.save()
            message = template.format(
                document.entity_type,
                document.entity_id,
                document.name,
                document.search_content,
                )

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
            search_content: str = data.get('name')
            search_content = search_content.lower()
            search_content = unidecode(search_content, "utf-8")
            search_content = unidecode(search_content)
            # was search_content = search_content.strip_diacritics(search_content)
            # TODO was peewee.fn.to_tsvector(search_content)
            data['search_content'] = search_content
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

    @classmethod
    def search_text(cls, search_string):
        search_string = search_string.lower()
        search_string = unidecode(search_string, "utf-8")
        search_string = unidecode(search_string)
        # was search_string = search_string.strip_diacritics(search_string)
        search_string = ','.join(search_string.split())
        query = (SqliteEntity
                 .select(SqliteEntity, SqliteEntity.bm25().alias('score'))
                 .where(SqliteEntity.search_content.match(search_string))
                 .order_by(SqliteEntity.bm25()))
        # query = SqliteEntity.raw("""
        #     SELECT entity_type,
        #         entity_id,
        #         name,
        #         ts_rank_cd(search_content, query, 63) AS rank
        #     FROM entities,
        #         to_tsquery(%s) query
        #     WHERE query @@ search_content
        #     ORDER BY rank DESC
        #     LIMIT 100
        #     """, search_string)
        return query

    @classmethod
    def string_to_tsvector(cls, string):
        string = string.lower()
        string = unidecode(string, "utf-8")
        string = unidecode(string)
        # was string = string.strip_diacritics(string)
        string = cls._strip_pattern.sub('', string)
        tsvector = string
        # TODO  was tsvector = peewee.fn.to_tsvector(string)
        return tsvector

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
                    relation = SqliteRelation(
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
                        relation = SqliteRelation(
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
                        relation = SqliteRelation(
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
                    relation = SqliteRelation(
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
                    relation = SqliteRelation(
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


SqliteEntity._tags_to_fields_mapping = {
    'aliases': ('aliases', SqliteEntity.element_to_names),
    'contact_info': ('contact_info', Bootstrapper.element_to_string),
    'groups': ('groups', SqliteEntity.element_to_names),
    'id': ('entity_id', Bootstrapper.element_to_integer),
    'members': ('members', SqliteEntity.element_to_names_and_ids),
    'name': ('name', Bootstrapper.element_to_string),
    'namevariations': ('name_variations', Bootstrapper.element_to_strings),
    'parentLabel': ('parent_label', SqliteEntity.element_to_parent_label),
    'profile': ('profile', Bootstrapper.element_to_string),
    'realname': ('real_name', Bootstrapper.element_to_string),
    'sublabels': ('sublabels', SqliteEntity.element_to_sublabels),
    'urls': ('urls', Bootstrapper.element_to_strings),
    }
