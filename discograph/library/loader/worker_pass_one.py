import logging
import multiprocessing

from sqlalchemy.exc import DatabaseError

from discograph.database import get_concurrency_count
from discograph.library.database.base_repository import BaseRepository
from discograph.library.database.database_helper import DatabaseHelper
from discograph.library.database.transaction import transaction

log = logging.getLogger(__name__)


class WorkerPassOne(multiprocessing.Process):
    def __init__(
        self,
        repository: BaseRepository,
        # model_class1,
        bulk_inserts1,
        # model_class2,
        # bulk_inserts2,
        inserted_count,
    ):
        super().__init__()
        self.repository = repository
        # self.model_class1 = model_class1
        self.bulk_inserts1 = bulk_inserts1
        # self.model_class2 = model_class2
        # self.bulk_inserts2 = bulk_inserts2
        self.inserted_count = inserted_count

    def run(self):
        proc_name = self.name

        if get_concurrency_count() > 1:
            DatabaseHelper.initialize()
            self.repository.__init__()

        with transaction():
            try:
                self.repository.save_all(self.bulk_inserts1)
                # session.add_all(self.bulk_inserts2)
            except DatabaseError:
                log.exception("Error in bootstrap_pass_one worker")
        log.info(f"[{proc_name}] inserted_count: {self.inserted_count}")

    # SPECIAL METHODS

    # def __repr__(self):
    #     def list_public_attributes(input_var):
    #         return {
    #             k: v
    #             for k, v in vars(input_var).items()
    #             if not (k.startswith("_") or k.startswith("random") or callable(v))
    #         }
    #
    #     return normalize_dict(list_public_attributes(self))

    # @classmethod
    # def loader_pass_one(cls, date: str):
    #     pass
    #
    # @classmethod
    # def updater_pass_one(cls, date: str):
    #     pass

    # @classmethod
    # def updater_pass_one_manager(
    #     cls,
    #     model_class,
    #     date: str,
    #     xml_tag: str,
    #     id_attr: str,
    #     name_attr: str,
    #     skip_without: List[str],
    # ):
    #     pass

    # @classmethod
    # def loader_pass_two(cls):
    #     pass

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

    # @classmethod
    # def preprocess_data(cls, data, element):
    #     return data

    # @classmethod
    # def tags_to_fields(cls, element, ignore_none=None, mapping=None):
    #     data = {}
    #     mapping = mapping or cls._tags_to_fields_mapping
    #     for child_element in element:
    #         entry = mapping.get(child_element.tag, None)
    #         if entry is None:
    #             continue
    #         field_name, procedure = entry
    #         value = procedure(child_element)
    #         if ignore_none and value is None:
    #             continue
    #         data[field_name] = value
    #     data = cls.preprocess_data(data, element)
    #     return data
