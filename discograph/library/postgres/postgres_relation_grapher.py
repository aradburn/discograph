import itertools

from discograph.library.postgres.postgres_entity import PostgresEntity
from discograph.library.postgres.postgres_relation import PostgresRelation
from discograph.library.relation_grapher import RelationGrapher


class PostgresRelationGrapher(RelationGrapher):

    __slots__ = (
        '_should_break_loop',
        '_center_entity',
        '_degree',
        '_entity_keys_to_visit',
        '_link_ratio',
        '_links',
        '_max_nodes',
        '_nodes',
        '_relational_roles',
        '_structural_roles',
    )

    # INITIALIZER

    def __init__(self, center_entity, degree=3, link_ratio=None, max_nodes=None, roles=None):
        assert isinstance(center_entity, PostgresEntity)
        super(PostgresRelationGrapher, self).__init__(center_entity, degree, link_ratio, max_nodes, roles)

    # SPECIAL METHODS

    def __call__(self):
        return super(PostgresRelationGrapher, self).__call__()

    def _cross_reference(self, distance):
        # TODO: We don't need to test all nodes, only those missing credit role
        #       relations. That may significantly reduce the computational
        #       load.
        if not self.relational_roles:
            print('    Skipping cross-referencing: no relational roles')
            return
        elif distance < 2:
            print('    Skipping cross-referencing: maximum distance less than 2')
            return
        else:
            print('    Cross-referencing...')
        relations = {}
        entity_keys = sorted(self.nodes)
        entity_keys.remove(self.center_entity.entity_key)
        entity_key_slices = []
        step = 250
        for start in range(0, len(entity_keys), step):
            entity_key_slices.append(entity_keys[start:start + step])
        iterator = itertools.product(entity_key_slices, entity_key_slices)
        for lh_entities, rh_entities in iterator:
            print('        {} & {}'.format(len(lh_entities), len(rh_entities)))
            found = PostgresRelation.search_bimulti(
                lh_entities,
                rh_entities,
                roles=self.relational_roles,
                )
            relations.update(found)
        self._process_relations(relations)
        message = '        Cross-referenced: {} nodes / {} links'
        message = message.format(len(self.nodes), len(self.links))
        print(message)

    @staticmethod
    def _search_entities(entity_keys_to_visit):
        print('        Retrieving entities')
        entities = []
        entity_keys_to_visit = list(entity_keys_to_visit)
        stop = len(entity_keys_to_visit)
        step = 1000
        for start in range(0, stop, step):
            entity_key_slice = entity_keys_to_visit[start:start + step]
            found = PostgresEntity.search_multi(entity_key_slice)
            entities.extend(found)
            message = '            {}-{} of {}'
            message = message.format(
                start + 1,
                min(start + step, stop),
                stop,
                )
            print(message)
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
                message = '            Pre-pruned {} [{}]'
                message = message.format(entity.name, relational_count)
                print(message)
        if provisional_roles and distance < self.degree:
            print('        Retrieving relational relations')
            keys = sorted(self.entity_keys_to_visit)
            step = 500
            stop = len(keys)
            for start in range(0, stop, step):
                key_slice = keys[start:start + step]
                print('            {}-{} of {}'.format(
                    start + 1,
                    min(start + step, stop),
                    stop,
                    ))
                relations.update(
                    PostgresRelation.search_multi(
                        key_slice,
                        roles=provisional_roles,
                        )
                    )
