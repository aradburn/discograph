from discograph import utils
from discograph.library import EntityType
from discograph.library.sqlite.sqlite_entity import SqliteEntity
from .sqlite_test_case import SqliteTestCase


class TestSqliteEntityStructuralRolesToRelations(SqliteTestCase):

    def test_01(self):
        entity = SqliteEntity.get(entity_type=EntityType.ARTIST, entity_id=430141)
        roles = ['Alias', 'Member Of']
        relations = entity.structural_roles_to_relations(roles)
        relations = [v for k, v in sorted(relations.items())]
        actual = '\n'.join(format(_) for _ in relations)
        expected = utils.normalize(u"""
            {
                "entity_one_id": 430141,
                "entity_one_type": "EntityType.ARTIST",
                "entity_two_id": 307,
                "entity_two_type": "EntityType.ARTIST",
                "random": null,
                "releases": null,
                "role": "Member Of"
            }
            {
                "entity_one_id": 430141,
                "entity_one_type": "EntityType.ARTIST",
                "entity_two_id": 3603,
                "entity_two_type": "EntityType.ARTIST",
                "random": null,
                "releases": null,
                "role": "Member Of"
            }
            """)
        assert actual == expected
