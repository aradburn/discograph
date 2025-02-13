import logging

from discograph.config import PostgresTestConfiguration
from discograph.runtime.data_access_layer.relation_grapher import RelationGrapher
from tests.integration.offline.database.repository_test_case import RepositoryTestCase

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
