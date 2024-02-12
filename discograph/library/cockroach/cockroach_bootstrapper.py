import logging

from discograph.library.cockroach.cockroach_entity import CockroachEntity
from discograph.library.cockroach.cockroach_relation import CockroachRelation
from discograph.library.cockroach.cockroach_release import CockroachRelease

log = logging.getLogger(__name__)


class CockroachBootstrapper:
    @classmethod
    def bootstrap_models(cls):
        log.debug("bootstrap cockroach models")
        CockroachEntity.drop_table(True)
        CockroachRelease.drop_table(True)
        CockroachRelation.drop_table(True)

        # log.debug("entity add index 1")
        # entity_idx1 = CockroachEntity.index(CockroachEntity.entity_type, CockroachEntity.name)
        # CockroachEntity.add_index(entity_idx1)
        # log.debug("entity add index 2")
        # entity_idx2 = CockroachEntity.index(CockroachEntity.name)
        # CockroachEntity.add_index(entity_idx2)
        # log.debug("entity add index 3")
        # entity_idx3 = CockroachEntity.index(CockroachEntity.search_content)
        # CockroachEntity.add_index(entity_idx3)

        CockroachEntity.create_table(True)
        CockroachRelease.create_table(True)
        CockroachRelation.create_table(True)

        log.debug("entity pass 1")
        CockroachEntity.bootstrap_pass_one()

        log.debug("release pass 1")
        CockroachRelease.bootstrap_pass_one()

        log.debug("entity pass 2")
        CockroachEntity.bootstrap_pass_two()

        log.debug("release pass 2")
        CockroachRelease.bootstrap_pass_two()

        log.debug("relation pass 1")
        CockroachRelation.bootstrap_pass_one()

        log.debug("entity pass 3")
        CockroachEntity.bootstrap_pass_three()

        log.debug("bootstrap done.")
