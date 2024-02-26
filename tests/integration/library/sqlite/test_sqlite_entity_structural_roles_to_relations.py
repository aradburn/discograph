import logging

from tests.integration.library.database.test_database_entity_structural_roles_to_relations import (
    TestDatabaseEntityStructuralRolesToRelations,
)
from tests.integration.library.sqlite.sqlite_test_case import SqliteTestCase

log = logging.getLogger(__name__)


class TestSqliteEntityStructuralRolesToRelations(
    SqliteTestCase, TestDatabaseEntityStructuralRolesToRelations
):
    # Run all tests in TestDatabaseEntityStructuralRolesToRelations
    pass
