from discograph.offline.database.entity_table import EntityTable
from discograph.offline.database.metadata_table import MetadataTable
from discograph.offline.database.relation_release_year_table import (
    RelationReleaseYearTable,
)
from discograph.offline.database.relation_table import RelationTable
from discograph.offline.database.release_table import ReleaseTable
from discograph.offline.database.role_table import RoleTable

ALL_OFFLINE_DATABASE_TABLES = [
    EntityTable,
    ReleaseTable,
    RelationTable,
    RoleTable,
    RelationReleaseYearTable,
    MetadataTable,
]
