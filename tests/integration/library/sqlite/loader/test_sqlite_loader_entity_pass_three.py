from tests.integration.library.loader.test_loader_entity_pass_three import (
    TestLoaderEntityPassThree,
)
from tests.integration.library.sqlite.sqlite_database_test_case import (
    SqliteDatabaseTestCase,
)


class TestSqliteLoaderEntityPassThree(
    SqliteDatabaseTestCase, TestLoaderEntityPassThree
):
    # Run all tests in TestLoaderEntityPassThree
    pass
