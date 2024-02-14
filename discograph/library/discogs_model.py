import gzip
import json
import logging
import multiprocessing
import pprint
import random

import peewee
from peewee import Model, FloatField, DatabaseProxy, DataError, fn
from playhouse.shortcuts import model_to_dict

import discograph.config
import discograph.database
import discograph.utils
from discograph.library.bootstrapper import Bootstrapper

log = logging.getLogger(__name__)

database_proxy = DatabaseProxy()  # Create a proxy for the database.


class DiscogsModel(Model):
    BULK_INSERT_BATCH_SIZE = 1000
    _tags_to_fields_mapping: dict = None

    class BootstrapPassOneWorker(multiprocessing.Process):
        def __init__(self, model_class, bulk_inserts, inserted_count):
            super().__init__()
            self.model_class = model_class
            self.bulk_inserts = bulk_inserts
            self.inserted_count = inserted_count

        def run(self):
            proc_name = self.name
            from discograph.database import bootstrap_database

            if bootstrap_database:
                database_proxy.initialize(bootstrap_database)
            with DiscogsModel.connection_context():
                try:
                    self.model_class.bulk_create(self.bulk_inserts)
                except peewee.PeeweeException:
                    log.exception("Error in bootstrap_pass_one worker")
            log.info(f"[{proc_name}] inserted_count: {self.inserted_count}")

    # PEEWEE FIELDS

    random = FloatField(index=True, null=True)

    # PEEWEE META

    class Meta:
        database = database_proxy

    # SPECIAL METHODS

    def __format__(self, format_specification=""):
        return json.dumps(model_to_dict(self), indent=4, sort_keys=True, default=str)

    def __repr__(self):
        return str(self)

    @classmethod
    def bootstrap_pass_one(
        cls, model_class, xml_tag, id_attr="id", name_attr="name", skip_without=None
    ):
        # Pass one.
        initial_count = len(model_class)
        inserted_count = 0
        xml_path = Bootstrapper.get_xml_path(xml_tag)
        log.info(f"Loading data from {xml_path}")
        with gzip.GzipFile(xml_path, "r") as file_pointer:
            iterator = Bootstrapper.iterparse(file_pointer, xml_tag)
            bulk_inserts = []
            workers = []
            for i, element in enumerate(iterator):
                data = None
                try:
                    data = model_class.tags_to_fields(element)
                    if skip_without:
                        if any(not data.get(_) for _ in skip_without):
                            continue
                    if element.get("id"):
                        data["id"] = element.get("id")
                    data["random"] = random.random()
                    # log.debug(**data)
                    new_instance = model_class(model_class, **data)
                    # log.debug(f"new_instance: {new_instance}", flush=True)
                    bulk_inserts.append(new_instance)
                    inserted_count += 1
                    if discograph.database.get_concurrency_count() > 1:
                        # Can do multi threading
                        if len(bulk_inserts) >= DiscogsModel.BULK_INSERT_BATCH_SIZE:
                            worker = cls.insert_bulk(
                                model_class, bulk_inserts, inserted_count
                            )
                            worker.start()
                            workers.append(worker)
                            bulk_inserts.clear()
                        if len(workers) > discograph.database.get_concurrency_count():
                            worker = workers.pop(0)
                            log.debug(f"wait for worker {len(workers)} in list")
                            worker.join()
                            if worker.exitcode > 0:
                                log.debug(
                                    f"worker {worker.name} exitcode: {worker.exitcode}"
                                )
                                # raise Exception("Error in worker process")
                            worker.terminate()
                    # if inserted_count >= 1000000:
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
                except DataError as e:
                    log.exception("Error in bootstrap_pass_one", pprint.pformat(data))
                    # traceback.print_exc()
                    raise e
            while len(workers) > 0:
                worker = workers.pop(0)
                log.debug(
                    f"wait for worker {worker.name} - {len(workers)} left in list"
                )
                worker.join()
                if worker.exitcode > 0:
                    log.debug(f"worker {worker.name} exitcode: {worker.exitcode}")
                    # raise Exception("Error in worker process")
                worker.terminate()
            if len(bulk_inserts) > 0:
                with DiscogsModel.atomic():
                    try:
                        model_class.bulk_create(bulk_inserts)
                    except peewee.PeeweeException as e:
                        log.exception("Error in bootstrap_pass_one")
                        raise e
            updated_count = len(model_class) - initial_count
            log.debug(f"inserted_count: {inserted_count}")
            log.debug(f"updated_count: {updated_count}")
            assert inserted_count == updated_count

    @classmethod
    def insert_bulk(cls, model_class, bulk_inserts, inserted_count):
        worker = cls.BootstrapPassOneWorker(model_class, bulk_inserts, inserted_count)
        return worker

    @classmethod
    def bootstrap_pass_two(cls, model_class, name_attr="name"):
        corpus = {}
        maximum_id = model_class.select(fn.Max(model_class.id)).scalar()
        for i in range(1, maximum_id + 1):
            query = model_class.select().where(model_class.id == i)
            if not query.count():
                continue
            document = list(query)[0]
            changed = document.resolve_references(corpus)
            if changed:
                log.debug(
                    f"{model_class.__name__.upper()}           (Pass 2) (id:{document.id}):\t"
                    + f"{getattr(document, name_attr)}"
                )
                document.save()
            else:
                log.debug(
                    f"{model_class.__name__.upper()} [SKIPPED] (Pass 2) (id:{document.id}):\t"
                    + f"{getattr(document, name_attr)}"
                )

    @staticmethod
    def database():
        return database_proxy

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
