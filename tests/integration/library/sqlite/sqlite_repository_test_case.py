import logging

from discograph.config import SqliteTestConfiguration
from discograph.library.relation_grapher import RelationGrapher
from tests.integration.library.database.repository_test_case import RepositoryTestCase

log = logging.getLogger(__name__)


class SqliteRepositoryTestCase(RepositoryTestCase):
    @classmethod
    def setUpClass(cls):
        RepositoryTestCase._config = SqliteTestConfiguration()
        # RepositoryTestCase.entity = SqliteEntityDB
        # RepositoryTestCase.relation = SqliteRelationDB
        # RepositoryTestCase.release = SqliteReleaseDB
        # RepositoryTestCase.role = RoleDB
        RepositoryTestCase.relation_grapher = RelationGrapher
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
