import atexit
import logging

from discograph.config import PostgresTestConfiguration
from discograph.database import setup_database, shutdown_database
from discograph.library.cache.cache_manager import setup_cache, shutdown_cache
from discograph.logging_config import setup_logging, shutdown_logging

log = logging.getLogger(__name__)


class PostgresUpdater:
    pass
    # @staticmethod
    # def update_models(date="", test=False):
    #     log.info("update postgres models")
    #
    #     log.debug("entity pass 1")
    #     # PostgresUpdater.update_pass_one(PostgresEntity)
    #
    #     # log.debug("entity analyze")
    #     # PostgresEntity.database().execute_sql("VACUUM FULL ANALYZE postgresentity;")
    #     #
    #     log.debug("release pass 1")
    #     # PostgresUpdater.update_pass_one(PostgresRelease)
    #
    #     # log.debug("release analyze")
    #     # PostgresRelease.database().execute_sql("VACUUM FULL ANALYZE postgresrelease;")
    #     #
    #     # log.debug("entity pass 2")
    #     # PostgresEntity.bootstrap_pass_two()
    #     #
    #     # log.debug("release pass 2")
    #     # PostgresRelease.bootstrap_pass_two()
    #     #
    #     # log.debug("relation pass 1")
    #     # PostgresRelation.bootstrap_pass_one()
    #     #
    #     # log.debug("relation analyze")
    #     # PostgresEntity.database().execute_sql("VACUUM FULL ANALYZE postgresentity;")
    #     # PostgresRelease.database().execute_sql("VACUUM FULL ANALYZE postgresrelease;")
    #     # PostgresRelation.database().execute_sql("VACUUM FULL ANALYZE postgresrelation;")
    #     #
    #     # log.debug("entity pass 3")
    #     # PostgresEntity.bootstrap_pass_three()
    #     #
    #     # log.debug("final vacuum analyze")
    #     # PostgresEntity.database().execute_sql("VACUUM FULL ANALYZE postgresentity;")
    #     # PostgresRelease.database().execute_sql("VACUUM FULL ANALYZE postgresrelease;")
    #     # PostgresRelation.database().execute_sql("VACUUM FULL ANALYZE postgresrelation;")
    #
    #     log.debug("update done.")

    # @staticmethod
    # def update_pass_one(model_class):
    #     PostgresUpdater.update_by_xml_tag_pass_one(
    #         model_class,
    #         xml_tag="artist",
    #         id_attr="entity_id",
    #         name_attr="name",
    #         skip_without=["name"],
    #     )
    #     PostgresUpdater.update_by_xml_tag_pass_one(
    #         model_class,
    #         xml_tag="label",
    #         id_attr="entity_id",
    #         name_attr="name",
    #         skip_without=["name"],
    #     )

    # @staticmethod
    # def update_release_pass_one(model_class):
    #     PostgresUpdater.update_by_xml_tag_pass_one(
    #         model_class,
    #         xml_tag="release",
    #         name_attr="title",
    #         skip_without=["title"],
    #     )

    # @staticmethod
    # def update_by_xml_tag_pass_one(
    #     model_class, xml_tag, id_attr="id", name_attr="name", skip_without=None
    # ):
    #     # Pass one.
    #     inserted_count = 0
    #     date = "test_initial"
    #     xml_path = LoaderUtils.get_xml_path(xml_tag, date)
    #     log.info(f"Loading data from {xml_path}")
    #     with gzip.GzipFile(xml_path, "r") as file_pointer:
    #         iterator = LoaderUtils.iterparse(file_pointer, xml_tag)
    #         bulk_inserts = []
    #         workers = []
    #         for i, element in enumerate(iterator):
    #             data = None
    #             try:
    #                 data = model_class.tags_to_fields(element)
    #                 if skip_without:
    #                     if any(not data.get(_) for _ in skip_without):
    #                         continue
    #                 if element.get("id"):
    #                     data["id"] = element.get("id")
    #
    #                 # log.debug(f"{pprint.pformat(data)}")
    #                 updated_entity = model_class(model_class, **data)
    #                 # PostgresUpdater.update_record(model_class, updated_entity)
    #
    #                 if i > 10000:
    #                     break
    #             except DataError as e:
    #                 log.exception("Error in bootstrap_pass_one", pprint.pformat(data))
    #                 raise e

    # @staticmethod
    # def update_record(model_class, updated_entity):
    #     with DiscogsModel.atomic():
    #         query = model_class.select().where(
    #             model_class.entity_id == updated_entity.entity_id,
    #             model_class.entity_type == updated_entity.entity_type,
    #         )
    #         if not query.count():
    #             log.debug("Entity not found")
    #             return
    #         old_entity = query.get()
    #         differences = DeepDiff(
    #             old_entity,
    #             updated_entity,
    #             exclude_paths=[
    #                 "entities",
    #                 "search_content",
    #                 "relation_counts",
    #                 "random",
    #                 "dirty_fields",
    #                 "_dirty",
    #             ],
    #             ignore_numeric_type_changes=True,
    #         )
    #         diff = pprint.pformat(differences)
    #         if diff != "{}":
    #             log.debug(f"diff: {diff}")
    #         # if pickle.dumps(old_entity) != pickle.dumps(updated_entity):
    #         #     log.debug(f"old: {old_entity}")
    #         #     log.debug(f"new: {updated_entity}")


if __name__ == "__main__":
    setup_logging()
    log.info("")
    config = vars(PostgresTestConfiguration)
    setup_cache(config)
    setup_database(config)
    # Note reverse order (last in first out), logging is the last to be shutdown
    atexit.register(shutdown_logging)
    atexit.register(shutdown_cache)
    atexit.register(shutdown_database)
    # PostgresUpdater.update_models()
