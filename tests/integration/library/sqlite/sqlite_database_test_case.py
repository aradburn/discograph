import logging

from discograph.config import SqliteTestConfiguration
from discograph.library.relation_grapher import RelationGrapher
from tests.integration.library.database.database_test_case import DatabaseTestCase

log = logging.getLogger(__name__)


class SqliteDatabaseTestCase(DatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        DatabaseTestCase._config = SqliteTestConfiguration()
        DatabaseTestCase.relation_grapher = RelationGrapher
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
