from discograph import utils
from discograph.library.database.relation_repository import RelationRepository
from discograph.library.database.release_info_repository import ReleaseInfoRepository
from discograph.library.database.release_repository import ReleaseRepository
from discograph.library.database.transaction import transaction
from discograph.library.domain.relation import Relation, RelationUncommitted
from discograph.library.domain.release import Release
from discograph.library.domain.release_info import ReleaseInfoUncommitted
from discograph.library.fields.entity_type import EntityType
from discograph.library.loader.loader_role import LoaderRole
from tests.integration.library.database.repository_test_case import RepositoryTestCase


class TestRepositoryRelation(RepositoryTestCase):
    def test_create_01(self):
        # GIVEN
        date = "test"
        LoaderRole().loader_pass_one(date)

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
                random=0.0,
            )
            # role = RoleRepository().get_by_name(relation.role_name)

        # WHEN
        with transaction():
            repository = RelationRepository()
            created_relation = repository.create(relation)
            print(f"created_relation: {created_relation}")

            release_info = ReleaseInfoUncommitted(
                relation_id=created_relation.relation_id,
                release_id=release.release_id,
                release_date=release.release_date,
            )
            created_release_info = ReleaseInfoRepository().create(release_info)
            print(f"created_release_info: {created_release_info}")

            repository.commit()

            retrieved_relation = repository.find_by_key(relation.model_dump())
            actual = utils.normalize_dict(retrieved_relation.model_dump())
            print(f"retrieved_relation: {retrieved_relation}")
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
        expected = utils.normalize_dict(expected_relation.model_dump())
        print(f"expected: {expected}")
        self.assertEqual(expected, actual)

    def test_get_01(self):
        # GIVEN

        # WHEN
        with transaction():
            repository = RelationRepository()
            # Get internal RelationDB
            relation_db = repository.get(1)
            # Convert to domain Relation
            relation = repository._to_domain(relation_db)
            actual = utils.normalize_dict(relation.model_dump())

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
        expected = utils.normalize_dict(expected_relation.model_dump())
        print(f"expected: {expected}")
        self.assertEqual(expected, actual)
