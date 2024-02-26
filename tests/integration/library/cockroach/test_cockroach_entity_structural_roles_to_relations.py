from tests.integration.library.cockroach.cockroach_test_case import CockroachTestCase
from tests.integration.library.database.test_database_entity_structural_roles_to_relations import (
    TestDatabaseEntityStructuralRolesToRelations,
)


class TestCockroackEntityStructuralRolesToRelations(
    CockroachTestCase, TestDatabaseEntityStructuralRolesToRelations
):
    # Run all tests in TestDatabaseEntityStructuralRolesToRelations
    pass
