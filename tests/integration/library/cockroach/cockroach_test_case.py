import logging
from unittest import SkipTest

import peewee
import psycopg2

from discograph.config import CockroachTestConfiguration
from discograph.library.cockroach.cockroach_entity import CockroachEntity
from discograph.library.cockroach.cockroach_relation import CockroachRelation
from discograph.library.cockroach.cockroach_relation_grapher import (
    CockroachRelationGrapher,
)
from discograph.library.cockroach.cockroach_release import CockroachRelease
from tests.integration.library.database.database_test_case import DatabaseTestCase

log = logging.getLogger(__name__)


class CockroachTestCase(DatabaseTestCase):
    @classmethod
    def setUpClass(cls):
        DatabaseTestCase._config = CockroachTestConfiguration()
        DatabaseTestCase.entity = CockroachEntity
        DatabaseTestCase.relation = CockroachRelation
        DatabaseTestCase.release = CockroachRelease
        DatabaseTestCase.relation_grapher = CockroachRelationGrapher
        try:
            super().setUpClass()
        except (psycopg2.OperationalError, peewee.OperationalError):
            cls.tearDownClass()
        finally:
            raise SkipTest("Cannot connect to Cockroach Database")

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
