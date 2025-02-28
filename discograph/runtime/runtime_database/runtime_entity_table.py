from sqlalchemy import String, JSON, Index, Integer, inspect
from sqlalchemy.orm import Mapped, mapped_column

from discograph import utils
from discograph.library.fields.entity_type import EntityType
from discograph.library.fields.int_enum import IntEnum
from discograph.runtime.runtime_database.runtime_base_table import RuntimeBase


class RuntimeEntityTable(RuntimeBase):
    """
    Represents the runtime entity table in the database.

    Attributes:
        id (int): The unique identifier for the runtime entity.
        entity_id (int): The ID of the entity.
        entity_type (EntityType): The type of the entity.
        entity_name (str): The name of the entity.
        relation_counts (dict | list): The relation counts of the entity.
        entity_metadata (dict | list): The metadata of the entity.
        aliases (dict | list): The aliases of the entity.
        groups (dict | list): The groups associated with the entity.
        members (dict | list): The members associated with the entity.
        countries (str): The countries associated with the entity.
        genres (str): The genres associated with the entity.
        styles (str): The styles associated with the entity.
    """

    __tablename__ = "runtime_entity"

    # COLUMNS

    id: Mapped[int] = mapped_column(primary_key=True)
    entity_id: Mapped[int] = mapped_column(Integer)
    entity_type: Mapped[EntityType] = mapped_column(IntEnum(EntityType), nullable=False)
    entity_name: Mapped[str] = mapped_column(String, nullable=False)
    relation_counts: Mapped[dict | list] = mapped_column(type_=JSON, nullable=True)
    entity_metadata: Mapped[dict | list] = mapped_column(type_=JSON, nullable=False)
    aliases: Mapped[dict | list] = mapped_column(type_=JSON, nullable=True)
    groups: Mapped[dict | list] = mapped_column(type_=JSON, nullable=True)
    members: Mapped[dict | list] = mapped_column(type_=JSON, nullable=True)
    countries: Mapped[str] = mapped_column(String, nullable=True)
    genres: Mapped[str] = mapped_column(String, nullable=True)
    styles: Mapped[str] = mapped_column(String, nullable=True)

    __table_args__ = (
        Index(
            "idx_runtime_entity_id_and_entity_type",
            entity_id,
            entity_type,
            unique=True,
        ),
        {},
    )

    def __init__(self, **entries):
        """
        Initializes a RuntimeEntityTable instance.

        Args:
            entries (dict): The entries to initialize the instance with.
        """
        column_names = set(
            [column.name for column in inspect(RuntimeEntityTable).columns]
        )
        superentries = {
            k: entries[k] for k in column_names.intersection(entries.keys())
        }
        super().__init__(**superentries)

    def __repr__(self):
        """
        Returns a string representation of the RuntimeEntityTable instance.

        Returns:
            str: The string representation of the instance.
        """
        return utils.normalize_dict(utils.row2dict(self), skip_keys={})
