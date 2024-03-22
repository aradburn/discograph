import gzip
import json
import logging
import multiprocessing
import pprint
import random
from typing import List

from peewee import Model, DataError, PeeweeException
from playhouse.shortcuts import model_to_dict

from discograph.database import get_concurrency_count
from discograph.library.database.database_worker import DatabaseWorker
from discograph.library.loader_utils import LoaderUtils

log = logging.getLogger(__name__)


class DiscogsModel(Model):
    BULK_INSERT_BATCH_SIZE = 1000
    BULK_UPDATE_BATCH_SIZE = 1000
    _tags_to_fields_mapping: dict = None

    class LoaderPassOneWorker(multiprocessing.Process):
        def __init__(
            self,
            model_class1,
            bulk_inserts1,
            model_class2,
            bulk_inserts2,
            inserted_count,
        ):
            super().__init__()
            self.model_class1 = model_class1
            self.bulk_inserts1 = bulk_inserts1
            self.model_class2 = model_class2
            self.bulk_inserts2 = bulk_inserts2
            self.inserted_count = inserted_count

        def run(self):
            proc_name = self.name
            from discograph.library.database.database_proxy import database_proxy

            database_proxy.initialize(DatabaseWorker.worker_database)
            with DiscogsModel.connection_context():
                try:
                    self.model_class1.bulk_create(self.bulk_inserts1)
                    self.model_class2.bulk_create(self.bulk_inserts2)
                except PeeweeException:
                    log.exception("Error in bootstrap_pass_one worker")
            log.info(f"[{proc_name}] inserted_count: {self.inserted_count}")

    # PEEWEE FIELDS

    # PEEWEE META

    class Meta:
        from discograph.library.database.database_proxy import database_proxy

        # DO NOT use the Peewee thread-safe metadata implementation.
        # model_metadata_class = ThreadSafeDatabaseMetadata
        database = database_proxy

    # SPECIAL METHODS

    def __format__(self, format_specification=""):
        return json.dumps(
            model_to_dict(self, exclude=["random"]),
            indent=4,
            sort_keys=True,
            default=str,
        )

    def __repr__(self):
        return str(self)

    @classmethod
    def loader_pass_one(cls, date: str):
        pass

    @classmethod
    def loader_pass_one_manager(
        cls,
        model_class,
        date: str,
        xml_tag: str,
        id_attr: str,
        name_attr: str,
        skip_without: List[str],
    ):
        from discograph.library.models.release_genre import ReleaseGenre

        # Loader pass one.
        initial_count = len(model_class)
        inserted_count = 0
        xml_path = LoaderUtils.get_xml_path(xml_tag, date)
        log.info(f"Loading data from {xml_path}")
        with gzip.GzipFile(xml_path, "r") as file_pointer:
            iterator = LoaderUtils.iterparse(file_pointer, xml_tag)
            bulk_inserts = []
            bulk_release_genre_inserts = []
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
                    log.debug(f"data: {data}")
                    if "genres" in data:
                        genres = data["genres"]
                        log.debug(f"process genres: {genres}")
                        del data["genres"]
                        new_instance = model_class(model_class, **data)

                        for genre in genres:
                            new_release_genre_instance = ReleaseGenre(
                                release_id=new_instance, genre_id=genre
                            )
                            bulk_release_genre_inserts.append(
                                new_release_genre_instance
                            )
                    else:
                        new_instance = model_class(model_class, **data)
                    log.debug(f"new_instance: {new_instance}")
                    bulk_inserts.append(new_instance)
                    inserted_count += 1
                    if get_concurrency_count() > 1:
                        # Can do multi threading
                        if len(bulk_inserts) >= DiscogsModel.BULK_INSERT_BATCH_SIZE:
                            worker = cls.insert_bulk(
                                model_class,
                                bulk_inserts,
                                ReleaseGenre,
                                bulk_release_genre_inserts,
                                inserted_count,
                            )
                            worker.start()
                            workers.append(worker)
                            bulk_inserts.clear()
                            bulk_release_genre_inserts.clear()
                        if len(workers) > get_concurrency_count():
                            worker = workers.pop(0)
                            log.debug(f"wait for worker {len(workers)} in list")
                            worker.join()
                            if worker.exitcode > 0:
                                log.debug(
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
                except DataError as e:
                    log.exception("Error in loader_pass_one", pprint.pformat(data))
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
                        ReleaseGenre.bulk_create(bulk_release_genre_inserts)
                    except PeeweeException as e:
                        log.exception("Error in loader_pass_one")
                        raise e
            updated_count = len(model_class) - initial_count
            log.debug(f"inserted_count: {inserted_count}")
            log.debug(f"updated_count: {updated_count}")
            assert inserted_count == updated_count

    @classmethod
    def insert_bulk(
        cls, model_class1, bulk_inserts1, model_class2, bulk_inserts2, inserted_count
    ):
        worker = cls.LoaderPassOneWorker(
            model_class1, bulk_inserts1, model_class2, bulk_inserts2, inserted_count
        )
        return worker

    @classmethod
    def updater_pass_one(cls, date: str):
        pass

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
        pass

    @classmethod
    def loader_pass_two(cls, model_class, name_attr="name"):
        pass

        # log.info("!!!!!!!!!!!!!!!!!!!!!!!")
        # corpus = {}
        # maximum_id = model_class.select(fn.Max(model_class.id)).scalar()
        # for i in range(1, maximum_id + 1):
        #     query = model_class.select().where(model_class.id == i)
        #     if not query.count():
        #         continue
        #     document = list(query)[0]
        #     changed = document.resolve_references(corpus)
        #     if changed:
        #         log.debug(
        #             f"{model_class.__name__.upper()}           (Pass 2) (id:{document.id}):\t"
        #             + f"{getattr(document, name_attr)}"
        #         )
        #         document.save()
        #     else:
        #         log.debug(
        #             f"{model_class.__name__.upper()} [SKIPPED] (Pass 2) (id:{document.id}):\t"
        #             + f"{getattr(document, name_attr)}"
        #         )

    @staticmethod
    def database():
        from discograph.library.database.database_proxy import database_proxy

        return database_proxy

    @staticmethod
    def connect():
        from discograph.library.database.database_proxy import database_proxy

        database_proxy.connect()

    @staticmethod
    def close():
        from discograph.library.database.database_proxy import database_proxy

        database_proxy.close()

    @staticmethod
    def connection_context():
        from discograph.library.database.database_proxy import database_proxy

        return database_proxy.connection_context()

    @staticmethod
    def atomic():
        from discograph.library.database.database_proxy import database_proxy

        return database_proxy.atomic()

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
