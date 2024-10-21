from discograph import utils
from discograph.config import TEST_DATA_DIR
from discograph.library.data_access_layer.relation_data_access import RelationDataAccess
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.database.relation_repository import RelationRepository
from discograph.library.database.transaction import transaction
from discograph.library.domain.entity import Entity
from discograph.library.domain.relation import (
    Relation,
    RelationInternal,
)
from discograph.library.fields.entity_type import EntityType
from discograph.library.loader.loader_entity import LoaderEntity
from discograph.library.loader.loader_role import LoaderRole
from discograph.library.loader.worker_relation_pass_one import WorkerRelationPassOne
from discograph.library.loader.loader_utils import LoaderUtils
from tests.integration.library.database.repository_test_case import RepositoryTestCase


class TestRepositoryRelation(RepositoryTestCase):
    def test_01_create(self):
        # GIVEN
        LoaderRole().load_all_roles()
        iterator = LoaderUtils.get_iterator(TEST_DATA_DIR, "artist", "testinsert")
        entity_element_1 = next(iterator)
        entity_1 = LoaderEntity().from_element(entity_element_1)
        entity_element_2 = next(iterator)
        entity_2 = LoaderEntity().from_element(entity_element_2)

        # WHEN
        with transaction():
            repository = EntityRepository()
            created_entity_1 = repository.create(entity_1)
            print(f"created_entity_1: {created_entity_1}")
            created_entity_2 = repository.create(entity_2)
            print(f"created_entity_2: {created_entity_2}")

        id_1 = Entity.to_entity_internal_id(
            created_entity_1.entity_id, created_entity_1.entity_type
        )
        id_2 = Entity.to_entity_internal_id(
            created_entity_2.entity_id, created_entity_2.entity_type
        )
        relation = RelationInternal(
            id=1,
            subject=id_1,
            object=id_2,
            role="Composed By",
            # releases={},
            random=0.0,
        )
        relation_dict = relation.model_dump()
        # relation_dict["role"] = relation.role_name
        relation_dicts = [relation_dict]

        # WHEN
        with transaction():
            relation_repository = RelationRepository()
            relations = WorkerRelationPassOne.to_relations_from_dict(relation_dicts)

            created_relation_internal = relation_repository.create(relations[0])
            print(f"created_relation_internal: {created_relation_internal}")

            created_relation = RelationDataAccess.to_relation(created_relation_internal)
            print(f"created_relation: {created_relation}")
            actual = utils.normalize_dict(
                created_relation.model_dump(exclude={"random"})
            )
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
            random=0.0,
        )
        expected = utils.normalize_dict(
            expected_relation.model_dump(exclude={"random"})
        )
        self.assertEqual(expected, actual)

    # def test_02_update(self):
    #     # GIVEN
    #     with transaction():
    #         release = Release.model_validate(
    #             {
    #                 "artists": [{"id": 939, "name": "Higher Intelligence Agency, The"}],
    #                 "companies": [],
    #                 "country": "UK",
    #                 "extra_artists": [
    #                     {
    #                         "id": 939,
    #                         "name": "Higher Intelligence Agency, The",
    #                         "roles": [{"name": "Written-By"}],
    #                     }
    #                 ],
    #                 "formats": [
    #                     {"descriptions": ["EP"], "name": "CD", "quantity": "1"}
    #                 ],
    #                 "genres": ["Electronic"],
    #                 "identifiers": [
    #                     {
    #                         "description": None,
    #                         "type": "Barcode",
    #                         "value": "5 018524 066308",
    #                     },
    #                     {
    #                         "description": None,
    #                         "type": "Matrix / Runout",
    #                         "value": "DISCTRONICS S HIA 2 CD 01",
    #                     },
    #                 ],
    #                 "labels": [
    #                     {"catalog_number": "HIACD2", "id": 233, "name": "Beyond"}
    #                 ],
    #                 "master_id": 21103,
    #                 "notes": None,
    #                 "random": 0.0,
    #                 "release_date": "1994-01-01 00:00:00",
    #                 "release_id": 635,
    #                 "styles": ["Techno", "Ambient"],
    #                 "title": "Colour Reform",
    #                 "tracklist": [
    #                     {
    #                         "duration": "8:49",
    #                         "extra_artists": [
    #                             {
    #                                 "id": 932,
    #                                 "name": "A Positive Life",
    #                                 "roles": [{"name": "Remix"}],
    #                             }
    #                         ],
    #                         "position": "1",
    #                         "title": "Universal Entity (Ketamine Entity Reformed By A Positive Life)",
    #                     },
    #                     {
    #                         "duration": "6:24",
    #                         "extra_artists": [
    #                             {
    #                                 "id": 41,
    #                                 "name": "Autechre",
    #                                 "roles": [{"name": "Remix"}],
    #                             }
    #                         ],
    #                         "position": "2",
    #                         "title": "Speech3 (Conoid Tone Reformed By Autechre)",
    #                     },
    #                     {
    #                         "duration": "8:30",
    #                         "extra_artists": [
    #                             {
    #                                 "id": 379334,
    #                                 "name": "Adrian Harrow",
    #                                 "roles": [{"name": "Engineer"}],
    #                             },
    #                             {
    #                                 "id": 953,
    #                                 "name": "Irresistible Force, The",
    #                                 "roles": [{"name": "Remix"}],
    #                             },
    #                         ],
    #                         "position": "3",
    #                         "title": "Speedlearn (Reformed By The Irresistible Force)",
    #                     },
    #                     {
    #                         "duration": "6:20",
    #                         "extra_artists": [
    #                             {
    #                                 "id": 954,
    #                                 "name": "Pentatonik",
    #                                 "roles": [{"name": "Remix"}],
    #                             }
    #                         ],
    #                         "position": "4",
    #                         "title": "Alpha 1999 (Delta Reformed By Pentatonik)",
    #                     },
    #                 ],
    #             }
    #         )
    #         ReleaseRepository().create(release)
    #
    #         relation = RelationUncommitted(
    #             entity_one_id=2,
    #             entity_one_type=EntityType.ARTIST,
    #             entity_two_id=3,
    #             entity_two_type=EntityType.LABEL,
    #             role_name="Composed By",
    #             releases={},
    #             random=0.0,
    #         )
    #         relation_dict = relation.model_dump()
    #         relation_dict["role"] = relation.role_name
    #
    #     # WHEN
    #     with transaction():
    #         relation_repository = RelationRepository()
    #
    #         WorkerRelationPassTwo.update_relation(
    #             relation_repository=relation_repository,
    #             relation_dict=relation_dict,
    #             release_id=635,
    #             year=1994,
    #         )
    #         updated_relation = relation_repository.find_by_key(relation_dict)
    #         print(f"updated_relation: {updated_relation}")
    #         actual = utils.normalize_dict(
    #             updated_relation.model_dump(exclude={"random"})
    #         )
    #         print(f"actual: {actual}")
    #
    #     # THEN
    #     expected_relation = Relation(
    #         relation_id=1,
    #         version_id=2,
    #         entity_one_id=2,
    #         entity_one_type=EntityType.ARTIST,
    #         entity_two_id=3,
    #         entity_two_type=EntityType.LABEL,
    #         role="Composed By",
    #         releases={"635": 1994},
    #         random=0.0,
    #     )
    #     expected = utils.normalize_dict(
    #         expected_relation.model_dump(exclude={"random"})
    #     )
    #     print(f"expected: {expected}")
    #     self.assertEqual(expected, actual)

    def test_03_get(self):
        # GIVEN

        # WHEN
        with transaction():
            repository = RelationRepository()
            # Get internal RelationDB
            relation_db = repository.get(1)
            # Convert to domain Relation
            relation_internal = repository._to_domain(relation_db)
            relation = RelationDataAccess.to_relation(relation_internal)
            print(f"relation: {relation}")
            actual = utils.normalize_dict(relation.model_dump(exclude={"random"}))

        # THEN
        expected_relation = Relation(
            id=1,
            entity_one_id=3,
            entity_one_type=EntityType.ARTIST,
            entity_two_id=5,
            entity_two_type=EntityType.ARTIST,
            role="Composed By",
            # releases={"635": 1994},
            random=0.0,
        )
        expected = utils.normalize_dict(
            expected_relation.model_dump(exclude={"random"})
        )
        print(f"expected: {expected}")
        self.assertEqual(expected, actual)
