from tests.integration.library.database.test_database_entity_structural_roles_to_relations import (
    TestDatabaseEntityStructuralRolesToRelations,
)
from tests.integration.library.postgres.postgres_test_case import PostgresTestCase


class TestPostgresEntityStructuralRolesToRelations(
    PostgresTestCase, TestDatabaseEntityStructuralRolesToRelations
):
    # Run all tests in TestDatabaseEntityStructuralRolesToRelations
    pass
