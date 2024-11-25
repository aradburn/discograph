from discograph.library.database.entity_repository import EntityRepository
from tests.integration.library.database.database_test_case import DatabaseTestCase


class TestLoaderEntityPassTwo(DatabaseTestCase):
    def test_loader_entity_pass_two(self):
        # GIVEN

        # WHEN
        actual = EntityRepository().count()

        # THEN
        expected = 6216
        self.assertEqual(expected, actual)
