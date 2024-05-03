from discograph import utils
from discograph.library.database.relation_repository import RelationRepository
from discograph.library.database.release_repository import ReleaseRepository
from discograph.library.database.transaction import transaction
from discograph.library.domain.relation import Relation, RelationUncommitted
from discograph.library.domain.release import Release
from discograph.library.fields.entity_type import EntityType
from discograph.library.loader.loader_role import LoaderRole
from discograph.library.loader.worker_relation_pass_one import WorkerRelationPassOne
from tests.integration.library.database.repository_test_case import RepositoryTestCase


class TestRepositoryRelation(RepositoryTestCase):
    def test_01_create(self):
        # GIVEN
        date = "test"
        LoaderRole().loader_pass_one(date)

        relation = RelationUncommitted(
            entity_one_id=2,
            entity_one_type=EntityType.ARTIST,
            entity_two_id=3,
            entity_two_type=EntityType.LABEL,
            role_name="Composed By",
            releases={},
            random=0.0,
        )
        relation_dict = relation.model_dump()
        relation_dict["role"] = relation.role_name

        # WHEN
        with transaction():
            relation_repository = RelationRepository()
            created_relation = WorkerRelationPassOne.create_relation(
                relation_repository=relation_repository,
                relation_dict=relation_dict,
            )
            # created_relation = relation_repository.create(relation)
            print(f"created_relation: {created_relation}")

            # retrieved_relation = relation_repository.find_by_key(relation.model_dump())
            # print(f"retrieved_relation: {retrieved_relation}")
            # actual = utils.normalize_dict(retrieved_relation.model_dump())

            actual = utils.normalize_dict(
                created_relation.model_dump(exclude={"random"})
            )
            print(f"actual: {actual}")

        # THEN
        expected_relation = Relation(
            relation_id=1,
            entity_one_id=2,
            entity_one_type=EntityType.ARTIST,
            entity_two_id=3,
            entity_two_type=EntityType.LABEL,
            role="Composed By",
            releases={},
            random=0.0,
        )
        expected = utils.normalize_dict(
            expected_relation.model_dump(exclude={"random"})
        )
        self.assertEqual(expected, actual)

    def test_02_update(self):
        # GIVEN
        with transaction():
            release = Release.model_validate(
                {
                    "artists": [{"id": 939, "name": "Higher Intelligence Agency, The"}],
                    "companies": [],
                    "country": "UK",
                    "extra_artists": [
                        {
                            "id": 939,
                            "name": "Higher Intelligence Agency, The",
                            "roles": [{"name": "Written-By"}],
                        }
                    ],
                    "formats": [
                        {"descriptions": ["EP"], "name": "CD", "quantity": "1"}
                    ],
                    "genres": ["Electronic"],
                    "identifiers": [
                        {
                            "description": None,
                            "type": "Barcode",
                            "value": "5 018524 066308",
                        },
                        {
                            "description": None,
                            "type": "Matrix / Runout",
                            "value": "DISCTRONICS S HIA 2 CD 01",
                        },
                    ],
                    "labels": [
                        {"catalog_number": "HIACD2", "id": 233, "name": "Beyond"}
                    ],
                    "master_id": 21103,
                    "notes": None,
                    "random": 0.0,
                    "release_date": "1994-01-01 00:00:00",
                    "release_id": 635,
                    "styles": ["Techno", "Ambient"],
                    "title": "Colour Reform",
                    "tracklist": [
                        {
                            "duration": "8:49",
                            "extra_artists": [
                                {
                                    "id": 932,
                                    "name": "A Positive Life",
                                    "roles": [{"name": "Remix"}],
                                }
                            ],
                            "position": "1",
                            "title": "Universal Entity (Ketamine Entity Reformed By A Positive Life)",
                        },
                        {
                            "duration": "6:24",
                            "extra_artists": [
                                {
                                    "id": 41,
                                    "name": "Autechre",
                                    "roles": [{"name": "Remix"}],
                                }
                            ],
                            "position": "2",
                            "title": "Speech3 (Conoid Tone Reformed By Autechre)",
                        },
                        {
                            "duration": "8:30",
                            "extra_artists": [
                                {
                                    "id": 379334,
                                    "name": "Adrian Harrow",
                                    "roles": [{"name": "Engineer"}],
                                },
                                {
                                    "id": 953,
                                    "name": "Irresistible Force, The",
                                    "roles": [{"name": "Remix"}],
                                },
                            ],
                            "position": "3",
                            "title": "Speedlearn (Reformed By The Irresistible Force)",
                        },
                        {
                            "duration": "6:20",
                            "extra_artists": [
                                {
                                    "id": 954,
                                    "name": "Pentatonik",
                                    "roles": [{"name": "Remix"}],
                                }
                            ],
                            "position": "4",
                            "title": "Alpha 1999 (Delta Reformed By Pentatonik)",
                        },
                    ],
                }
            )
            ReleaseRepository().create(release)

            relation = RelationUncommitted(
                entity_one_id=2,
                entity_one_type=EntityType.ARTIST,
                entity_two_id=3,
                entity_two_type=EntityType.LABEL,
                role_name="Composed By",
                releases={},
                random=0.0,
            )
            # role = RoleRepository().get_by_name(relation.role_name)

        # WHEN
        with transaction():
            relation_repository = RelationRepository()
            retrieved_relation = relation_repository.find_by_key(relation.model_dump())
            print(f"retrieved_relation: {retrieved_relation}")
            updated_relation = WorkerRelationPassOne.update_relation(
                relation_repository=relation_repository,
                relation=retrieved_relation,
                release_id=635,
                year=1994,
            )
            print(f"updated_relation: {updated_relation}")
            actual = utils.normalize_dict(
                updated_relation.model_dump(exclude={"random"})
            )
            print(f"actual: {actual}")

        # THEN
        expected_relation = Relation(
            relation_id=1,
            entity_one_id=2,
            entity_one_type=EntityType.ARTIST,
            entity_two_id=3,
            entity_two_type=EntityType.LABEL,
            role="Composed By",
            releases={"635": 1994},
            random=0.0,
        )
        expected = utils.normalize_dict(
            expected_relation.model_dump(exclude={"random"})
        )
        print(f"expected: {expected}")
        self.assertEqual(expected, actual)

    def test_03_get(self):
        # GIVEN

        # WHEN
        with transaction():
            repository = RelationRepository()
            # Get internal RelationDB
            relation_db = repository.get(1)
            # Convert to domain Relation
            relation = repository._to_domain(relation_db)
            actual = utils.normalize_dict(relation.model_dump(exclude={"random"}))

        # THEN
        expected_relation = Relation(
            relation_id=1,
            entity_one_id=2,
            entity_one_type=EntityType.ARTIST,
            entity_two_id=3,
            entity_two_type=EntityType.LABEL,
            role="Composed By",
            releases={"635": 1994},
            random=0.0,
        )
        expected = utils.normalize_dict(
            expected_relation.model_dump(exclude={"random"})
        )
        print(f"expected: {expected}")
        self.assertEqual(expected, actual)
