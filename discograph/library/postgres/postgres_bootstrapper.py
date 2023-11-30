from discograph.library.postgres.postgres_entity import PostgresEntity
from discograph.library.postgres.postgres_relation import PostgresRelation
from discograph.library.postgres.postgres_release import PostgresRelease


class PostgresBootstrapper:
    @classmethod
    def bootstrap_models(cls, pessimistic=False):
        print("bootstrap postgres models")
        # PostgresEntity.drop_table(True)
        # PostgresRelease.drop_table(True)
        # PostgresRelation.drop_table(True)
        #
        # PostgresEntity.create_table(True)
        # PostgresRelease.create_table(True)
        # PostgresRelation.create_table(True)
        #
        # print("entity pass 1")
        # PostgresEntity.bootstrap_pass_one()
        #
        # print("entity analyze")
        # PostgresEntity._meta.database.execute_sql("VACUUM FULL ANALYZE postgresentity;")
        #
        # print("release pass 1")
        # PostgresRelease.bootstrap_pass_one()
        #
        # print("release analyze")
        # PostgresRelease._meta.database.execute_sql("VACUUM FULL ANALYZE postgresrelease;")
        #
        # print("entity pass 2")
        # PostgresEntity.bootstrap_pass_two(pessimistic=pessimistic)
        #
        # print("release pass 2")
        # PostgresRelease.bootstrap_pass_two(pessimistic=pessimistic)
        #
        # print("relation pass 1")
        # PostgresRelation.bootstrap_pass_one(pessimistic=pessimistic)
        #
        # print("relation analyze")
        # PostgresEntity._meta.database.execute_sql("VACUUM FULL ANALYZE postgresentity;")
        # PostgresRelease._meta.database.execute_sql("VACUUM FULL ANALYZE postgresrelease;")
        # PostgresRelation._meta.database.execute_sql("VACUUM FULL ANALYZE postgresrelation;")

        # print("entity pass 3")
        # PostgresEntity.bootstrap_pass_three(pessimistic=pessimistic)
        #
        # print("final vacuum analyze")
        # PostgresEntity._meta.database.execute_sql("VACUUM FULL ANALYZE postgresentity;")
        # PostgresRelease._meta.database.execute_sql("VACUUM FULL ANALYZE postgresrelease;")
        # PostgresRelation._meta.database.execute_sql("VACUUM FULL ANALYZE postgresrelation;")

        print("bootstrap done.")
