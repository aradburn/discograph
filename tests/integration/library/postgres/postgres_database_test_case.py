import logging

from discograph.config import PostgresTestConfiguration
from discograph.library.relation_grapher import RelationGrapher
from tests.integration.library.database.database_test_case import DatabaseTestCase

log = logging.getLogger(__name__)


class PostgresDatabaseTestCase(DatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        DatabaseTestCase._config = PostgresTestConfiguration()
        DatabaseTestCase.relation_grapher = RelationGrapher
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
