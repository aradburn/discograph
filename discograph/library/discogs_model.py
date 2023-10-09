import gzip
import json
import pprint
import random
import traceback

from abjad import Timer
from peewee import Model, FloatField, DatabaseProxy, DataError, fn
from playhouse.shortcuts import model_to_dict

from discograph.library.bootstrapper import Bootstrapper

database_proxy = DatabaseProxy()  # Create a proxy for the database.


class DiscogsModel(Model):

    # PEEWEE FIELDS

    random = FloatField(index=True, null=True)

    # PEEWEE META

    class Meta:
        database = database_proxy

    # SPECIAL METHODS

    def __format__(self, format_specification=''):
        return json.dumps(model_to_dict(self), indent=4, sort_keys=True, default=str)

    def __repr__(self):
        return str(self)

    @classmethod
    def bootstrap_pass_one(cls, model_class, xml_tag, id_attr='id', name_attr='name', skip_without=None):
        # Pass one.
        initial_count = len(model_class)
        inserted_count = 0
        template = u'{} (Pass 1) (idx:{}) (id:{}) [{:.8f}]: {}'
        xml_path = Bootstrapper.get_xml_path(xml_tag)
        print(f"Loading data from {xml_path}", flush=True)
        with gzip.GzipFile(xml_path, 'r') as file_pointer:
            iterator = Bootstrapper.iterparse(file_pointer, xml_tag)
            for i, element in enumerate(iterator):
                data = None
                try:
                    with Timer(verbose=False) as timer:
                        data = model_class.tags_to_fields(element)
                        if skip_without:
                            if any(not data.get(_) for _ in skip_without):
                                continue
                        if element.get('id'):
                            data['id'] = element.get('id')
                        data['random'] = random.random()
                        document = model_class.create(**data)
                        inserted_count += 1
                    message = template.format(
                        model_class.__name__.upper(),
                        i,
                        getattr(document, id_attr),
                        timer.elapsed_time,
                        getattr(document, name_attr),
                        )
                    print(f"message: {message}")
                except DataError as e:
                    print("Error in bootstrap_pass_one")
                    pprint.pprint(data)
                    traceback.print_exc()
                    raise e
            updated_count = len(model_class) - initial_count
            print(f"inserted_count: {inserted_count}", flush=True)
            print(f"updated_count: {updated_count}", flush=True)
            print(f"i+1: {i+1}", flush=True)
            assert inserted_count == updated_count

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
            message = changed_template.format(
                model_class.__name__.upper(),
                document.id,
                timer.elapsed_time,
                getattr(document, name_attr),
                )
            print(message)
            document.save()

    @staticmethod
    def connect():
        database_proxy.connect()

    @staticmethod
    def connection_context():
        # return database_proxy.atomic()
        # return database_proxy.__enter__()
        return database_proxy.connection_context()

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
