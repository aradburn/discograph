import logging

from discograph.library.postgres.postgres_entity import PostgresEntity
from discograph.library.postgres.postgres_relation import PostgresRelation
from discograph.library.postgres.postgres_release import PostgresRelease

log = logging.getLogger(__name__)


class PostgresBootstrapper:
    @classmethod
    def bootstrap_models(cls):
        log.info("bootstrap postgres models")
        PostgresEntity.drop_table(True)
        PostgresRelease.drop_table(True)
        PostgresRelation.drop_table(True)

        PostgresEntity.create_table(True)
        PostgresRelease.create_table(True)
        PostgresRelation.create_table(True)

        log.debug("entity pass 1")
        PostgresEntity.bootstrap_pass_one()

        log.debug("entity analyze")
        PostgresEntity.database().execute_sql("VACUUM FULL ANALYZE postgresentity;")

        log.debug("release pass 1")
        PostgresRelease.bootstrap_pass_one()

        log.debug("release analyze")
        PostgresRelease.database().execute_sql("VACUUM FULL ANALYZE postgresrelease;")

        log.debug("entity pass 2")
        PostgresEntity.bootstrap_pass_two()

        log.debug("release pass 2")
        PostgresRelease.bootstrap_pass_two()

        log.debug("relation pass 1")
        PostgresRelation.bootstrap_pass_one()

        log.debug("relation analyze")
        PostgresEntity.database().execute_sql("VACUUM FULL ANALYZE postgresentity;")
        PostgresRelease.database().execute_sql("VACUUM FULL ANALYZE postgresrelease;")
        PostgresRelation.database().execute_sql("VACUUM FULL ANALYZE postgresrelation;")

        log.debug("entity pass 3")
        PostgresEntity.bootstrap_pass_three()

        log.debug("final vacuum analyze")
        PostgresEntity.database().execute_sql("VACUUM FULL ANALYZE postgresentity;")
        PostgresRelease.database().execute_sql("VACUUM FULL ANALYZE postgresrelease;")
        PostgresRelation.database().execute_sql("VACUUM FULL ANALYZE postgresrelation;")

        log.debug("bootstrap done.")
