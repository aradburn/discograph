# -*- encoding: utf-8 -*-
import gzip

import abjad
from peewee import *
import pprint
import random
import traceback

from playhouse.sqlite_ext import SqliteExtDatabase

from discograph.library.Bootstrapper import Bootstrapper
from abjad import Timer


# TODO  AJR was database = pool.PostgresqlExtDatabase(
#     'discograph',
#     host='127.0.0.1',
#     user=app.config['POSTGRESQL_USERNAME'],
#     password=app.config['POSTGRESQL_PASSWORD'],
#     )

DATABASE = 'discograph.db'

# Create a database instance that will manage the connection and
# execute queries
database = SqliteExtDatabase(DATABASE)


class PostgresModel(Model):

    # PEEWEE FIELDS

    random = FloatField(index=True, null=True)

    # PEEWEE META

    class Meta:
        database = database

    # SPECIAL METHODS

    def __format__(self, format_specification=''):
        # if format_specification in ('', 'storage'):
        #     return abjad.format.get_storage(self)
        return str(self)

    def __repr__(self):
        # return abjad.format.get_repr(self)
        return str(self)

    def __format__(self, format_specification=''):
        from abjad.tools import systemtools
        if format_specification in ('', 'storage'):
            return systemtools.StorageFormatManager.get_storage_format(self)
        return str(self)

    def __repr__(self):
        from abjad.tools import systemtools
        return systemtools.StorageFormatManager.get_repr_format(self)

    # PRIVATE PROPERTIES

    # @property
    # def _storage_format_specification(self):
    #     keyword_argument_names = sorted(self._meta.fields)
    #     # if 'id' in keyword_argument_names:
    #     #    keyword_argument_names.remove('id')
    #     for keyword_argument_name in keyword_argument_names[:]:
    #         value = getattr(self, keyword_argument_name)
    #         if isinstance(value, list) and not value:
    #             keyword_argument_names.remove(keyword_argument_name)
    #         elif isinstance(value, peewee.Function):
    #             keyword_argument_names.remove(keyword_argument_name)
    #     return StorageFormatSpecification(
    #         self,
    #         keyword_argument_names=keyword_argument_names,
    #         )

    @property
    def _repr_specification(self):
        return self._storage_format_specification

    # PUBLIC METHODS

    @classmethod
    def bootstrap_postgres_models(cls, pessimistic=False):
        import discograph
        discograph.PostgresEntity.drop_table(True)
        discograph.PostgresRelease.drop_table(True)
        discograph.PostgresRelation.drop_table(True)
        discograph.PostgresEntity.create_table(True)
        discograph.PostgresRelease.create_table(True)
        discograph.PostgresRelation.create_table(True)
        discograph.PostgresEntity.bootstrap_pass_one()
        discograph.PostgresEntity.bootstrap_pass_two(pessimistic=pessimistic)
        discograph.PostgresRelease.bootstrap_pass_one()
        discograph.PostgresRelease.bootstrap_pass_two(pessimistic=pessimistic)
        discograph.PostgresRelation.bootstrap_pass_one(pessimistic=pessimistic)
        discograph.PostgresEntity.bootstrap_pass_three(pessimistic=pessimistic)

    @classmethod
    def bootstrap_pass_one(cls, model_class, xml_tag, id_attr='id', name_attr='name', skip_without=None):
        # Pass one.
        template = u'{} (Pass 1) (idx:{}) (id:{}) [{:.8f}]: {}'
        xml_path = Bootstrapper.get_xml_path(xml_tag)
        print(xml_path)
        with gzip.GzipFile(xml_path, 'r') as file_pointer:
            iterator = Bootstrapper.iterparse(file_pointer, xml_tag)
            for i, element in enumerate(iterator):
                data = None
                print("bootstrap")
                try:
                    with Timer(verbose=True) as timer:
                        data = model_class.tags_to_fields(element)
                        if skip_without:
                            if any(not data.get(_) for _ in skip_without):
                                continue
                        if element.get('id'):
                            data['id'] = element.get('id')
                        data['random'] = random.random()
                        document = model_class.create(**data)
                    message = template.format(
                        model_class.__name__.upper(),
                        i,
                        getattr(document, id_attr),
                        timer.elapsed_time,
                        getattr(document, name_attr),
                        )
                    print(f"message: {message}")
                except DataError as e:
                    pprint.pprint(data)
                    traceback.print_exc()
                    raise e

    @classmethod
    def bootstrap_pass_two(cls, model_class, name_attr='name'):
        skipped_template = u'{} [SKIPPED] (Pass 2) (id:{}) [{:.8f}]: {}'
        changed_template = u'{}           (Pass 2) (id:{}) [{:.8f}]: {}'
        corpus = {}
        maximum_id = model_class.select(fn.Max(model_class.id)).scalar()
        for i in range(1, maximum_id + 1):
            query = model_class.select().where(model_class.id == i)
            if not query.count():
                continue
            document = list(query)[0]
            with Timer(verbose=False) as timer:
                changed = document.resolve_references(corpus)
            if not changed:
                message = skipped_template.format(
                    model_class.__name__.upper(),
                    document.id,
                    timer.elapsed_time,
                    getattr(document, name_attr),
                    )
                print(message)
                continue
            document.save()
            message = changed_template.format(
                model_class.__name__.upper(),
                document.id,
                timer.elapsed_time,
                getattr(document, name_attr),
                )
            print(message)

    @staticmethod
    def connect():
        database.connect()

    @classmethod
    def get_random(cls):
        n = random.random()
        return cls.select().where(cls.random > n).order_by(cls.random).get()

    @classmethod
    def preprocess_data(cls, data, element):
        return data

    @classmethod
    def tags_to_fields(cls, element, ignore_none=None, mapping=None):
        data = {}
        mapping = mapping or cls._tags_to_fields_mapping
        for child_element in element:
            entry = mapping.get(child_element.tag, None)
            if entry is None:
                continue
            field_name, procedure = entry
            value = procedure(child_element)
            if ignore_none and value is None:
                continue
            data[field_name] = value
        data = cls.preprocess_data(data, element)
        return data
