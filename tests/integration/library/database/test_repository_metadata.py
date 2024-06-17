import datetime

from discograph import utils
from discograph.library.database.metadata_repository import MetadataRepository
from discograph.library.database.transaction import transaction
from discograph.library.domain.metadata import Metadata, MetadataUncommitted
from tests.integration.library.database.repository_test_case import RepositoryTestCase


class TestRepositoryMetadata(RepositoryTestCase):
    def test_create_01(self):
        # GIVEN
        timestamp = datetime.datetime(year=2024, month=6, day=1)
        metadata = MetadataUncommitted(
            metadata_key="key1",
            metadata_value="value1",
            metadata_timestamp=timestamp,
        )

        # WHEN
        with transaction():
            repository = MetadataRepository()
            created_metadata = repository.create(metadata)
            actual = utils.normalize_dict(
                created_metadata.model_dump(exclude={"metadata_timestamp"})
            )
            print(f"actual: {actual}")

        # THEN
        expected_relation = Metadata(
            metadata_id=1,
            version_id=1,
            metadata_key="key1",
            metadata_value="value1",
            metadata_timestamp=timestamp,
        )
        expected = utils.normalize_dict(
            expected_relation.model_dump(exclude={"metadata_timestamp"})
        )
        self.assertEqual(expected, actual)

    def test_get_01(self):
        # GIVEN
        timestamp = datetime.datetime(year=2024, month=6, day=1)
        metadata = MetadataUncommitted(
            metadata_key="key2",
            metadata_value="value2",
            metadata_timestamp=timestamp,
        )

        # WHEN
        with transaction():
            repository = MetadataRepository()
            created_metadata = repository.create(metadata)

            retrieved_metadata = repository.get_by_key(metadata_key="key2")

        # THEN
        self.assertEqual(created_metadata, retrieved_metadata)
