import logging

from discograph.config import SqliteTestConfiguration
from discograph.library.models.role import Role
from discograph.library.sqlite.sqlite_entity import SqliteEntity
from discograph.library.sqlite.sqlite_relation import SqliteRelation
from discograph.library.sqlite.sqlite_relation_grapher import SqliteRelationGrapher
from discograph.library.sqlite.sqlite_release import SqliteRelease
from tests.integration.library.database.database_test_case import DatabaseTestCase

log = logging.getLogger(__name__)


class SqliteTestCase(DatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        DatabaseTestCase._config = SqliteTestConfiguration()
        DatabaseTestCase.entity = SqliteEntity
        DatabaseTestCase.relation = SqliteRelation
        DatabaseTestCase.release = SqliteRelease
        DatabaseTestCase.role = Role
        DatabaseTestCase.relation_grapher = SqliteRelationGrapher
        super(SqliteTestCase, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
