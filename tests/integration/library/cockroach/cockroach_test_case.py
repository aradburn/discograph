import logging
from unittest import SkipTest

from sqlalchemy.exc import DatabaseError

from discograph.config import CockroachTestConfiguration
from discograph.library.relation_grapher import RelationGrapher
from tests.integration.library.database.database_test_case import DatabaseTestCase

log = logging.getLogger(__name__)


class CockroachTestCase(DatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        DatabaseTestCase._config = CockroachTestConfiguration()
        # DatabaseTestCase.entity = CockroachEntityDB
        # DatabaseTestCase.relation = CockroachRelationDB
        # DatabaseTestCase.release = CockroachReleaseDB
        # DatabaseTestCase.role = RoleDB
        DatabaseTestCase.relation_grapher = RelationGrapher
        try:
            super().setUpClass()
        except DatabaseError:
            cls.tearDownClass()
        finally:
            raise SkipTest("Cannot connect to Cockroach Database")

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
