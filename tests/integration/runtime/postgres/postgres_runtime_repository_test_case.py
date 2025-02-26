import logging

from discograph.config import PostgresRuntimeTestConfiguration
from discograph.runtime.data_access_layer.relation_grapher import RelationGrapher
from tests.integration.runtime.database.runtime_repository_test_case import (
    RuntimeRepositoryTestCase,
)

log = logging.getLogger(__name__)


class PostgresRuntimeRepositoryTestCase(RuntimeRepositoryTestCase):
    @classmethod
    def setUpClass(cls):
        RuntimeRepositoryTestCase._runtime_config = PostgresRuntimeTestConfiguration()
        RuntimeRepositoryTestCase.relation_grapher = RelationGrapher
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
