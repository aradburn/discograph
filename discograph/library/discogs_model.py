import gzip
import json
import multiprocessing
import pprint
import random
import traceback

import peewee
from abjad import Timer
from peewee import Model, FloatField, DatabaseProxy, DataError, fn
from playhouse.shortcuts import model_to_dict

from discograph.library.bootstrapper import Bootstrapper

database_proxy = DatabaseProxy()  # Create a proxy for the database.


class DiscogsModel(Model):

    class BootstrapPassOneWorker(multiprocessing.Process):

        def __init__(self, model_class, bulk_inserts, inserted_count):
            super().__init__()
            self.model_class = model_class
            self.bulk_inserts = bulk_inserts
            self.inserted_count = inserted_count

        def run(self):
            proc_number = self.name.split('-')[-1]
            corpus = {}
            total = len(self.bulk_inserts)
            from discograph.helpers import bootstrap_database
            database_proxy.initialize(bootstrap_database)
            with DiscogsModel.connection_context():
                try:
                    with DiscogsModel.atomic():
                        self.model_class.bulk_create(self.bulk_inserts)
                except peewee.PeeweeException:
                    print("Error in bootstrap_pass_one worker")
                    traceback.print_exc()
            print(f"inserted_count: {self.inserted_count}")

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
            bulk_inserts = []
            workers = []
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
                        # print(**data)
                        new_instance = model_class(model_class, **data)
                        # print(f"new_instance: {new_instance}", flush=True)
                        bulk_inserts.append(new_instance)
                        # document = model_class.create(**data)
                        inserted_count += 1
                        if len(bulk_inserts) >= 50000:
                            worker = cls.insert_bulk(model_class, bulk_inserts, inserted_count)
                            worker.start()
                            workers.append(worker)
                            bulk_inserts.clear()
                        if len(workers) > multiprocessing.cpu_count():
                            worker = workers.pop(0)
                            # print(f"wait for worker {len(workers)} in list", flush=True)
                            worker.join()
                            if worker.exitcode > 0:
                                print(f"worker.exitcode: {worker.exitcode}")
                                raise Exception("Error in worker process")
                            worker.terminate()
                        if inserted_count > 2000000:
                            break
                    # message = template.format(
                    #     model_class.__name__.upper(),
                    #     i,
                    #     getattr(document, id_attr),
                    #     timer.elapsed_time,
                    #     getattr(document, name_attr),
                    #     )
                    # if Bootstrapper.is_test:
                    #     print(message)
                except DataError as e:
                    print("Error in bootstrap_pass_one")
                    pprint.pprint(data)
                    traceback.print_exc()
                    raise e
            while len(workers) > 0:
                worker = workers.pop(0)
                # print(f"wait for worker {len(workers)} in list", flush=True)
                worker.join()
                if worker.exitcode > 0:
                    print(f"worker.exitcode: {worker.exitcode}")
                    raise Exception("Error in worker process")
                worker.terminate()
            if len(bulk_inserts) > 0:
                with DiscogsModel.atomic():
                    try:
                        model_class.bulk_create(bulk_inserts)
                    except peewee.PeeweeException:
                        print("Error in bootstrap_pass_one")
                        traceback.print_exc()
            updated_count = len(model_class) - initial_count
            print(f"inserted_count: {inserted_count}", flush=True)
            print(f"updated_count: {updated_count}", flush=True)
            print(f"i+1: {i+1}", flush=True)
            assert inserted_count == updated_count

    @classmethod
    def insert_bulk(cls, model_class, bulk_inserts, inserted_count):
        worker = cls.BootstrapPassOneWorker(model_class, bulk_inserts, inserted_count)
        # print("bootstrap pass one - start worker")
        return worker

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
                if Bootstrapper.is_test:
                    print(message)
                continue
            message = changed_template.format(
                model_class.__name__.upper(),
                document.id,
                timer.elapsed_time,
                getattr(document, name_attr),
                )
            if Bootstrapper.is_test:
                print(message)
            document.save()

    @staticmethod
    def connect():
        database_proxy.connect()

    @staticmethod
    def close():
        database_proxy.close()

    @staticmethod
    def connection_context():
        return database_proxy.connection_context()

    @staticmethod
    def atomic():
        return database_proxy.atomic()
        # from discograph.app import app
        # from discograph.config import ThreadingModel
        # return database_proxy.atomic() if app.config['THREADING_MODEL'] == ThreadingModel.PROCESS else nullcontext()

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
