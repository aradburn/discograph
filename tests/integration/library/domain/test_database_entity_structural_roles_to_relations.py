import logging

from discograph import utils
from discograph.library.data_access_layer.entity_data_access import EntityDataAccess
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.database.transaction import transaction
from discograph.library.fields.entity_type import EntityType
from tests.integration.library.database.database_test_case import DatabaseTestCase

log = logging.getLogger(__name__)


class TestDatabaseEntityStructuralRolesToRelations(DatabaseTestCase):
    def test_01(self):
        entity_id = 430141
        entity_type = EntityType.ARTIST
        with transaction():
            entity = EntityRepository().get(entity_id, entity_type)
            print(f"entity: {entity}")
            roles = ["Alias", "Member Of"]
            relations = EntityDataAccess.structural_roles_to_relations(
                entity, roles=roles
            )
            print(f"relations: {relations}")
            actual = utils.normalize_dict(relations)
            print(f"actual: {actual}")

        expected_relations = {
            "artist-430141-member-of-artist-307": {
                "entity_one_id": 430141,
                "entity_one_type": EntityType.ARTIST,
                "entity_two_id": 307,
                "entity_two_type": EntityType.ARTIST,
                "releases": None,
                "role_id": None,
            },
            "artist-430141-member-of-artist-3603": {
                "entity_one_id": 430141,
                "entity_one_type": EntityType.ARTIST,
                "entity_two_id": 3603,
                "entity_two_type": EntityType.ARTIST,
                "releases": None,
                "role_id": None,
            },
        }
        expected = utils.normalize_dict(expected_relations)
        self.assertEqual(expected, actual)
