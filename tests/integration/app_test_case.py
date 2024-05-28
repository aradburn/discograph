import logging

from discograph.app import setup_application, shutdown_application
from discograph.config import PostgresTestConfiguration
from discograph.library.relation_grapher import RelationGrapher
from tests.integration.library.database.database_test_case import DatabaseTestCase

log = logging.getLogger(__name__)


class AppTestCase(DatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        DatabaseTestCase._config = PostgresTestConfiguration()
        DatabaseTestCase.relation_grapher = RelationGrapher
        super().setUpClass()
        setup_application()

    @classmethod
    def tearDownClass(cls):
        shutdown_application()
        super().tearDownClass()
