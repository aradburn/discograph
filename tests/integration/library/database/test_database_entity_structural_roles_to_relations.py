import logging

from discograph import utils
from discograph.library.entity_type import EntityType
from tests.integration.library.database.database_test_case import DatabaseTestCase

log = logging.getLogger(__name__)


class TestDatabaseEntityStructuralRolesToRelations(DatabaseTestCase):
    def test_01(self):
        entity = DatabaseTestCase.entity.get(
            entity_type=EntityType.ARTIST, entity_id=430141
        )
        roles = ["Alias", "Member Of"]
        relations = entity.structural_roles_to_relations(roles)
        relations = [v for k, v in sorted(relations.items())]
        actual = utils.normalize_str_list([format(_) for _ in relations])
        expected_entitys = [
            {
                "entity_one_id": 430141,
                "entity_one_type": "EntityType.ARTIST",
                "entity_two_id": 307,
                "entity_two_type": "EntityType.ARTIST",
                "random": None,
                "releases": None,
                "role": "Member Of",
            },
            {
                "entity_one_id": 430141,
                "entity_one_type": "EntityType.ARTIST",
                "entity_two_id": 3603,
                "entity_two_type": "EntityType.ARTIST",
                "random": None,
                "releases": None,
                "role": "Member Of",
            },
        ]
        expected = utils.normalize_dict_list(expected_entitys)
        self.assertEqual(expected, actual)
