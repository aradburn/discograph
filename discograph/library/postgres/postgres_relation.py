import logging

from discograph.library.models.relation import Relation

log = logging.getLogger(__name__)


class PostgresRelation(Relation):
    pass
    # __tablename__ = "relation"
    # __mapper_args__ = {"polymorphic_identity": "PostgresRelation"}
    #
    # # COLUMNS
    #
    # entity_one_id: Mapped[int] = mapped_column(primary_key=True)
    # entity_one_type: Mapped[EntityType] = mapped_column(
    #     IntEnum(EntityType),
    #     primary_key=True,
    # )
    # entity_two_id: Mapped[int] = mapped_column(primary_key=True)
    # entity_two_type: Mapped[EntityType] = mapped_column(
    #     IntEnum(EntityType),
    #     primary_key=True,
    # )
    # role: Mapped[str] = mapped_column(String, primary_key=True)
    # releases: Mapped[dict | list] = mapped_column(type_=JSON, nullable=False)
    # random: Mapped[float] = mapped_column(Float)

    # __table_args__ = (
    #     PrimaryKeyConstraint(
    #         Relation.entity_one_id,
    #         Relation.entity_one_type,
    #         Relation.entity_two_id,
    #         Relation.entity_two_type,
    #         Relation.role,
    #     ),
    #     Index(
    #         "idx_entity_one_id",
    #         Relation.entity_one_id,
    #         postgresql_using="hash",
    #         unique=False,
    #     ),
    #     Index(
    #         "idx_entity_two_id",
    #         Relation.entity_two_id,
    #         postgresql_using="hash",
    #         unique=False,
    #     ),
    #     {"extend_existing": True},
    # )


# idx_entity_one_id = PostgresRelation.index(
#     PostgresRelation.entity_one_id, unique=False, safe=True, using="hash"
# )
# PostgresRelation.add_index(idx_entity_one_id)
#
# # Create an index on entity_two_id
# idx_entity_two_id = PostgresRelation.index(
#     PostgresRelation.entity_two_id, unique=False, safe=True, using="hash"
# )
# PostgresRelation.add_index(idx_entity_two_id)
