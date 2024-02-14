import itertools
import logging

from discograph.library.cockroach.cockroach_entity import CockroachEntity
from discograph.library.cockroach.cockroach_relation import CockroachRelation
from discograph.library.relation_grapher import RelationGrapher

log = logging.getLogger(__name__)


class CockroachRelationGrapher(RelationGrapher):
    # __slots__ = (
    #     "_should_break_loop",
    #     "_center_entity",
    #     "_degree",
    #     "_entity_keys_to_visit",
    #     "_link_ratio",
    #     "_links",
    #     "_max_nodes",
    #     "_nodes",
    #     "_relational_roles",
    #     "_structural_roles",
    # )

    # INITIALIZER

    def __init__(
        self, center_entity, degree=3, link_ratio=None, max_nodes=None, roles=None
    ):
        assert isinstance(center_entity, CockroachEntity)
        super(CockroachRelationGrapher, self).__init__(
            center_entity, degree, link_ratio, max_nodes, roles
        )

    # SPECIAL METHODS

    def __call__(self):
        return super(CockroachRelationGrapher, self).__call__()

    def _cross_reference(self, distance):
        # TODO: We don't need to test all nodes, only those missing credit role
        #       relations. That may significantly reduce the computational
        #       load.
        if not self.relational_roles:
            log.debug("    Skipping cross-referencing: no relational roles")
            return
        elif distance < 2:
            log.debug("    Skipping cross-referencing: maximum distance less than 2")
            return
        else:
            log.debug("    Cross-referencing...")
        relations = {}
        entity_keys = sorted(self.nodes)
        entity_keys.remove(self.center_entity.entity_key)
        entity_key_slices = []
        step = 250
        for start in range(0, len(entity_keys), step):
            entity_key_slices.append(entity_keys[start : start + step])
        iterator = itertools.product(entity_key_slices, entity_key_slices)
        for lh_entities, rh_entities in iterator:
            log.debug(f"        lh: {len(lh_entities)} rh: {len(rh_entities)}")
            found = CockroachRelation.search_bimulti(
                lh_entities,
                rh_entities,
                roles=self.relational_roles,
            )
            relations.update(found)
        self._process_relations(relations)
        log.debug(
            f"        Cross-referenced: {len(self.nodes)} nodes / {len(self.links)} links"
        )

    @staticmethod
    def _search_entities(entity_keys_to_visit):
        log.debug("        Retrieving entities")
        entities = []
        entity_keys_to_visit = list(entity_keys_to_visit)
        stop = len(entity_keys_to_visit)
        step = 1000
        for start in range(0, stop, step):
            entity_key_slice = entity_keys_to_visit[start : start + step]
            found = CockroachEntity.search_multi(entity_key_slice)
            entities.extend(found)
            log.debug(f"            {start + 1}-{min(start + step, stop)} of {stop}")
        return entities

    def _search_via_relational_roles(self, distance, provisional_roles, relations):
        for entity_key in sorted(self.entity_keys_to_visit):
            node = self.nodes.get(entity_key)
            if not node:
                continue
            entity = node.entity
            relational_count = entity.roles_to_relation_count(provisional_roles)
            if 0 < distance and self.max_links < relational_count:
                self.entity_keys_to_visit.remove(entity_key)
                log.debug(f"            Pre-pruned {entity.name} [{relational_count}]")
        if provisional_roles and distance < self.degree:
            log.debug("        Retrieving relational relations")
            keys = sorted(self.entity_keys_to_visit)
            step = 500
            stop = len(keys)
            for start in range(0, stop, step):
                key_slice = keys[start : start + step]
                log.debug(
                    f"            {start + 1}-{min(start + step, stop)} of {stop}"
                )
                relations.update(
                    CockroachRelation.search_multi(
                        key_slice,
                        roles=provisional_roles,
                    )
                )
