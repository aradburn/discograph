from tests.integration.library.loader.test_loader_entity_pass_one import (
    TestLoaderEntityPassOne,
)
from tests.integration.library.sqlite.sqlite_database_test_case import (
    SqliteDatabaseTestCase,
)


class TestSqliteLoaderEntityPassOne(SqliteDatabaseTestCase, TestLoaderEntityPassOne):
    # Run all tests in TestLoaderEntityPassOne
    pass
