import logging

from sqlalchemy import select

from discograph import utils
from discograph.library.fields.entity_type import EntityType
from discograph.library.models.entity import Entity
from tests.integration.library.database.database_test_case import DatabaseTestCase

log = logging.getLogger(__name__)


class TestDatabaseEntityStructuralRolesToRelations(DatabaseTestCase):
    def test_01(self):
        pk = (430141, EntityType.ARTIST)
        with self.test_session.begin() as session:
            entity: Entity = session.scalars(
                select(DatabaseTestCase.entity).where(
                    (DatabaseTestCase.entity.entity_id == pk[0])
                    & (DatabaseTestCase.entity.entity_type == pk[1])
                )
            ).one()
            print(f"entity: {entity}")
            roles = ["Alias", "Member Of"]
            relations = entity.structural_roles_to_relations(roles=roles)
            print(f"relations: {relations}")
            # relations = {
            #     key: {
            #         k: v
            #         for k, v in vars(rel).items()
            #         if not (k.startswith("_") or k.startswith("random") or callable(v))
            #     }
            #     for key, rel in sorted(relations.items())
            # }
            # relations = dict(sorted(utils.row2dict(relations.items())))
            # sorted_relations = dict(sorted(relations.items()))
            relations = {k: utils.row2dict(v) for k, v in sorted(relations.items())}
            print(f"relations: {relations}")
            actual = utils.normalize_dict(relations)
            print(f"actual: {actual}")

        expected_relations = """
            {
                "artist-430141-member-of-artist-307": {
                    "entity_one_id": 430141,
                    "entity_one_type": "EntityType.ARTIST",
                    "entity_two_id": 307,
                    "entity_two_type": "EntityType.ARTIST",
                    "random": null,
                    "releases": null,
                    "role": "Member Of"
                },
                "artist-430141-member-of-artist-3603": {
                    "entity_one_id": 430141,
                    "entity_one_type": "EntityType.ARTIST",
                    "entity_two_id": 3603,
                    "entity_two_type": "EntityType.ARTIST",
                    "random": null,
                    "releases": null,
                    "role": "Member Of"
                }
            }
        """
        expected = utils.strip_input(expected_relations)
        self.assertEqual(expected, actual)
