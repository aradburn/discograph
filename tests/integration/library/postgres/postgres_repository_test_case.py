import logging

from discograph.config import PostgresTestConfiguration
from discograph.library.relation_grapher import RelationGrapher
from tests.integration.library.database.repository_test_case import RepositoryTestCase

log = logging.getLogger(__name__)


class PostgresRepositoryTestCase(RepositoryTestCase):
    @classmethod
    def setUpClass(cls):
        RepositoryTestCase._config = PostgresTestConfiguration()
        RepositoryTestCase.relation_grapher = RelationGrapher
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
