from sqlalchemy import (
    ForeignKey,
    Index,
    Integer,
    inspect,
)
from sqlalchemy.orm import Mapped, mapped_column

from discograph import utils
from discograph.runtime.runtime_database import RuntimeRoleTable
from discograph.runtime.runtime_database.runtime_base_table import RuntimeBase


class RuntimeRelationTable(RuntimeBase):
    __tablename__ = "runtime_relation"

    # COLUMNS

    id: Mapped[int] = mapped_column(primary_key=True)
    subject: Mapped[int] = mapped_column(Integer)
    predicate: Mapped[int] = mapped_column(ForeignKey(RuntimeRoleTable.id))
    object: Mapped[int] = mapped_column(Integer)

    __table_args__ = (
        Index(
            "idx_runtime_relation",
            subject,
            predicate,
            object,
            unique=True,
        ),
        Index(
            "idx_runtime_relation_subject",
            subject,
            unique=False,
        ),
        Index(
            "idx_runtime_relation_object",
            object,
            unique=False,
        ),
        {},
    )

    def __init__(self, **entries):
        """Override to avoid TypeError when passed spurious column names"""
        column_names = set(
            [column.name for column in inspect(RuntimeRelationTable).columns]
        )
        superentries = {
            k: entries[k] for k in column_names.intersection(entries.keys())
        }
        super().__init__(**superentries)

    def __repr__(self):
        return utils.normalize_dict(utils.row2dict(self), skip_keys={})
