from discograph import utils
from discograph.config import TEST_DATA_DIR
from discograph.library.fields.entity_id import to_entity_internal_id
from discograph.offline.data_access_layer.role_data_access import RoleDataAccess
from discograph.offline.database.entity_repository import EntityRepository
from discograph.offline.database.offline_transaction import offline_transaction
from discograph.offline.database.relation_repository import RelationRepository
from discograph.offline.domain.relation import (
    Relation,
    RelationInternal,
    RelationUncommitted,
)
from discograph.offline.loader.loader_role import LoaderRole
from discograph.offline.loader.loader_utils import LoaderUtils
from discograph.offline.loader.parser_entity import ParserEntity
from tests.integration.offline.database.offline_repository_test_case import (
    OfflineRepositoryTestCase,
)


class TestRepositoryRelation(OfflineRepositoryTestCase):
    def test_01_create(self):
        # GIVEN
        LoaderRole.load_roles_into_database()
        RoleDataAccess.load_all_roles()
        iterator = LoaderUtils.get_iterator(TEST_DATA_DIR, "artist", "testinsert")
        entity_element_1 = next(iterator)
        entity_1 = ParserEntity().from_element(entity_element_1)
        entity_element_2 = next(iterator)
        entity_2 = ParserEntity().from_element(entity_element_2)

        # WHEN
        with offline_transaction():
            repository = EntityRepository()
            created_entity_1 = repository.create(entity_1)
            print(f"created_entity_1: {created_entity_1}")
            created_entity_2 = repository.create(entity_2)
            print(f"created_entity_2: {created_entity_2}")

        id_1 = to_entity_internal_id(
            created_entity_1.entity_id, created_entity_1.entity_type
        )
        id_2 = to_entity_internal_id(
            created_entity_2.entity_id, created_entity_2.entity_type
        )
        relation = RelationInternal(
            id=1,
            subject=id_1,
            object=id_2,
            role="Composed By",
            # releases={},
        )
        relation_dict = relation.model_dump()
        # relation_dict["role"] = relation.role_name
        relation_dicts = [relation_dict]

        # WHEN
        with offline_transaction():
            relation_repository = RelationRepository()
            relations = RelationUncommitted.from_dicts(relation_dicts)

            created_relation_internal = relation_repository.create(relations[0])
            print(f"created_relation_internal: {created_relation_internal}")

            created_relation = created_relation_internal.to_relation()
            print(f"created_relation: {created_relation}")
            actual = utils.normalize_dict(created_relation.model_dump())
            print(f"actual: {actual}")

        # THEN
        expected_relation = Relation(
            id=1,
            entity_one_id=created_entity_1.entity_id,
            entity_one_type=created_entity_1.entity_type,
            entity_two_id=created_entity_2.entity_id,
            entity_two_type=created_entity_2.entity_type,
            role="Composed By",
            # releases={},
        )
        expected = utils.normalize_dict(expected_relation.model_dump())
        self.assertEqual(expected, actual)
