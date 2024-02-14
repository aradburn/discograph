import logging

from discograph.library.sqlite.sqlite_entity import SqliteEntity
from discograph.library.sqlite.sqlite_relation import SqliteRelation
from discograph.library.sqlite.sqlite_release import SqliteRelease


log = logging.getLogger(__name__)


class SqliteBootstrapper:
    @classmethod
    def bootstrap_models(cls):
        log.info("Bootstrap sqlite models")

        SqliteEntity.drop_table(True)
        SqliteRelease.drop_table(True)
        SqliteRelation.drop_table(True)
        SqliteEntity.create_table(True)
        SqliteRelease.create_table(True)
        SqliteRelation.create_table(True)
        SqliteEntity.bootstrap_pass_one()
        SqliteEntity.bootstrap_pass_two()
        SqliteRelease.bootstrap_pass_one()
        SqliteRelease.bootstrap_pass_two()
        SqliteRelation.bootstrap_pass_one()
        SqliteEntity.bootstrap_pass_three()
