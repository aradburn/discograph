from discograph import utils
from discograph.library.database.relation_release_year_repository import (
    RelationReleaseYearRepository,
)
from discograph.library.database.transaction import transaction
from discograph.library.domain.relation_release_year import (
    RelationReleaseYearUncommitted,
    RelationReleaseYear,
)
from discograph.library.loader.loader_role import LoaderRole
from tests.integration.library.database.repository_test_case import RepositoryTestCase


class TestRepositoryRelationReleaseYear(RepositoryTestCase):
    def test_01_create(self):
        # GIVEN
        LoaderRole().load_all_roles()

        relation_release_year = RelationReleaseYearUncommitted(
            relation_id=2,
            release_id=3,
            year=1969,
        )

        # WHEN
        with transaction():
            repository = RelationReleaseYearRepository()

            created_relation_release_year = repository.create(relation_release_year)
            print(f"created_relation_release_year: {created_relation_release_year}")

            actual = utils.normalize_dict(
                created_relation_release_year.model_dump(exclude={"random"})
            )
            print(f"actual: {actual}")

        # THEN
        expected_relation = RelationReleaseYear(
            relation_release_year_id=1,
            relation_id=2,
            release_id=3,
            year=1969,
        )
        expected = utils.normalize_dict(
            expected_relation.model_dump(exclude={"random"})
        )
        self.assertEqual(expected, actual)

    # def test_02_update(self):
    #     # GIVEN
    #     with transaction():
    #         release = ReleaseReleaseYear.model_validate(
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
            repository = RelationReleaseYearRepository()
            # Get internal RelationReleaseYearDB
            relation_release_years = repository.get(2)
            actual = [
                utils.normalize_dict(
                    relation_release_year.model_dump(exclude={"random"})
                )
                for relation_release_year in relation_release_years
            ]

        # THEN
        expected_relation_release_years = [
            RelationReleaseYear(
                relation_release_year_id=1,
                relation_id=2,
                release_id=3,
                year=1969,
            )
        ]
        expected = [
            utils.normalize_dict(
                expected_relation_release_year.model_dump(exclude={"random"})
            )
            for expected_relation_release_year in expected_relation_release_years
        ]
        print(f"expected: {expected}")
        self.assertListEqual(expected, actual)
