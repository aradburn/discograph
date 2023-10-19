from discograph.library.postgres.postgres_entity import PostgresEntity
from discograph.library.postgres.postgres_relation import PostgresRelation
from discograph.library.postgres.postgres_release import PostgresRelease


class PostgresBootstrapper:
    @classmethod
    def bootstrap_models(cls, pessimistic=False):
        print("bootstrap postgres models")
        PostgresEntity.drop_table(True)
        PostgresRelease.drop_table(True)
        PostgresRelation.drop_table(True)
        PostgresEntity.create_table(True)
        PostgresRelease.create_table(True)
        PostgresRelation.create_table(True)
        PostgresEntity.bootstrap_pass_one()
        PostgresRelease.bootstrap_pass_one()
        PostgresEntity.bootstrap_pass_two(pessimistic=pessimistic)
        PostgresRelease.bootstrap_pass_two(pessimistic=pessimistic)
        PostgresRelation.bootstrap_pass_one(pessimistic=pessimistic)
        PostgresEntity.bootstrap_pass_three(pessimistic=pessimistic)
