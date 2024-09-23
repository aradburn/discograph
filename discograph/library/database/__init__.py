from discograph.library.database.entity_table import EntityTable
from discograph.library.database.metadata_table import MetadataTable
from discograph.library.database.relation_release_year_table import (
    RelationReleaseYearTable,
)
from discograph.library.database.relation_table import RelationTable
from discograph.library.database.release_table import ReleaseTable
from discograph.library.database.role_table import RoleTable

ALL_DATABASE_TABLES = [
    EntityTable,
    ReleaseTable,
    RelationTable,
    RoleTable,
    RelationReleaseYearTable,
    MetadataTable,
]
