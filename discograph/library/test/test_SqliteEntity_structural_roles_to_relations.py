from abjad import string

import discograph


class Test(discograph.DiscographSqliteTestCase):

    def test_01(self):
        entity = discograph.SqliteEntity.get(entity_type=1, entity_id=430141)
        roles = ['Alias', 'Member Of']
        relations = entity.structural_roles_to_relations(roles)
        relations = [v for k, v in sorted(relations.items())]
        actual = '\n'.join(format(_) for _ in relations)
        expected = string.normalize(u"""
            {
                "entity_one_id": 430141,
                "entity_one_type": 1,
                "entity_two_id": 307,
                "entity_two_type": 1,
                "random": null,
                "releases": null,
                "role": "Member Of"
            }
            {
                "entity_one_id": 430141,
                "entity_one_type": 1,
                "entity_two_id": 3603,
                "entity_two_type": 1,
                "random": null,
                "releases": null,
                "role": "Member Of"
            }
            """)
        assert actual == expected
