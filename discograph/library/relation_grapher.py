import collections
import logging
from abc import ABC
from typing import List, Dict, Tuple, OrderedDict

from discograph import utils
from discograph.library.data_access_layer.entity_data_access import EntityDataAccess
from discograph.library.data_access_layer.role_data_access import RoleDataAccess
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.database.relation_repository import RelationRepository
from discograph.library.domain.entity import Entity
from discograph.library.domain.relation import RelationResult
from discograph.library.fields.entity_type import EntityType
from discograph.library.trellis_node import TrellisNode

log = logging.getLogger(__name__)


class RelationGrapher(ABC):
    # CLASS VARIABLES
    from discograph.library.database.database_helper import DatabaseHelper

    __slots__ = (
        "should_break_loop",
        "center_entity",
        "degree",
        "entity_keys_to_visit",
        "link_ratio",
        "links",
        "max_nodes",
        "nodes",
        "relational_role_names",
        "structural_role_names",
    )

    roles_to_prune = [
        "Released On",
        "Compiled On",
        "Producer",
        "Remix",
        "DJ Mix",
        "Written-By",
    ]

    # INITIALIZER

    def __init__(
        self,
        center_entity: Entity,
        degree: int = DatabaseHelper.MAX_DEGREE,
        link_ratio: int = None,
        max_nodes: int = None,
        role_names: List[str] = None,
    ):
        from discograph.library.database.database_helper import DatabaseHelper

        log.debug(
            f"RelationGrapher for {center_entity.entity_type}-{center_entity.entity_name}"
        )
        self.center_entity = center_entity
        degree = int(degree)
        assert degree > 0
        self.degree = degree
        if max_nodes is not None:
            max_nodes = int(max_nodes)
            assert max_nodes > 0
        else:
            max_nodes = DatabaseHelper.MAX_NODES
        self.max_nodes = max_nodes
        if link_ratio is not None:
            link_ratio = int(link_ratio)
            assert link_ratio > 0
        else:
            link_ratio = DatabaseHelper.LINK_RATIO
        self.link_ratio = link_ratio
        role_names = role_names or []
        self.structural_role_names: List[str] = []
        self.relational_role_names: List[str] = []
        if role_names:
            # if isinstance(role_names, str):
            #     role_names = (role_names,)
            # elif not isinstance(role_names, collections_abc.Iterable):
            #     role_names = (role_names,)
            # role_names = tuple(role_names)
            assert all(
                _ in RoleDataAccess.role_name_to_role_id_lookup.keys()
                for _ in role_names
            )
            for role_name in role_names:
                if role_name in ("Alias", "Sublabel Of", "Member Of"):
                    self.structural_role_names.append(role_name)
                else:
                    self.relational_role_names.append(role_name)
        # self.structural_role_names = tuple(structural_role_names)
        # self.relational_role_names = tuple(relational_role_names)
        self.nodes: OrderedDict[Tuple[int, EntityType], TrellisNode] = (
            collections.OrderedDict()
        )
        self.links: Dict[str, RelationResult] = {}
        self.should_break_loop = False
        self.entity_keys_to_visit = set[tuple[int, EntityType]]()

    def get_relation_graph(self):
        log.debug(f"Searching around {self.center_entity.entity_name}...")
        provisional_role_names = self.relational_role_names
        # provisional_roles = list(self.relational_role_names)
        self.report_search_start()
        self.clear()
        self.entity_keys_to_visit.add(self.center_entity.entity_key)
        distance = 0
        for distance in range(self.degree + 1):
            self.report_search_loop_start(distance)
            log.debug(f"    Search for: {self.entity_keys_to_visit}")
            entities = self.search_entities(self.entity_keys_to_visit)
            # log.debug(f"    Search found entities: {entities}")
            relations: Dict[str, RelationResult] = {}
            self.process_entities(distance, entities)
            if not self.entity_keys_to_visit or self.should_break_loop:
                break
            self.test_loop_one(distance)
            self.prune_roles(distance, provisional_role_names)
            if not self.should_break_loop:
                self.search_via_structural_roles(
                    distance, provisional_role_names, relations
                )
                self.search_via_relational_roles(
                    distance, provisional_role_names, relations
                )
            self.test_loop_two(distance, relations)
            self.entity_keys_to_visit.clear()
            self.process_relations(relations)
        self.build_trellis()
        # self.cross_reference(distance)
        pages = self.partition_trellis(distance)
        self.page_entities(pages)
        self.find_clusters()
        for node in self.nodes.values():
            expected_count = EntityDataAccess.roles_to_relation_count(
                node.entity, self.all_roles
            )
            node.missing = expected_count - len(node.links)
        # log.debug(f"self.links: {self.links}")
        # log.debug(f"self.nodes: {self.nodes}")
        json_links = tuple(
            link.as_json()
            for key, link in sorted(self.links.items(), key=lambda x: x[0])
        )
        json_nodes = tuple(
            node.as_json()
            for key, node in sorted(self.nodes.items(), key=lambda x: x[0])
        )
        network = {
            "center": {
                "key": self.center_entity.json_entity_key,
                "name": self.center_entity.entity_name,
            },
            "links": json_links,
            "nodes": json_nodes,
            "pages": len(pages),
        }
        return network

    @staticmethod
    def search_entities(
        entity_keys_to_visit: set[tuple[int, EntityType]]
    ) -> List[Entity]:
        log.debug(f"        Retrieving entities keys: {entity_keys_to_visit}")
        entities: List[Entity] = []
        entity_keys_to_visit = list(entity_keys_to_visit)
        stop = len(entity_keys_to_visit)
        step = 1000
        for start in range(0, stop, step):
            entity_key_slice = entity_keys_to_visit[start : start + step]
            found = EntityRepository().search_multi(entity_key_slice)
            entities.extend(found)
            log.debug(f"            {start + 1}-{min(start + step, stop)} of {stop}")
        return entities

    def search_via_relational_roles(
        self, distance, provisional_roles, relation_links: Dict[str, RelationResult]
    ):
        for entity_key in sorted(self.entity_keys_to_visit):
            node = self.nodes.get(entity_key)
            if not node:
                continue
            entity = node.entity
            relational_count = EntityDataAccess.roles_to_relation_count(
                entity, provisional_roles
            )
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
                relation_results = RelationRepository().search_multi(
                    entity_keys=key_slice, role_names=provisional_roles
                )
                for relation in relation_results:
                    relation_links[relation.link_key] = RelationResult(
                        relation_id=relation.relation_id,
                        entity_one_id=relation.entity_one_id,
                        entity_one_type=relation.entity_one_type,
                        entity_two_id=relation.entity_two_id,
                        entity_two_type=relation.entity_two_type,
                        random=relation.random,
                        releases=relation.releases,
                        role=relation.role,
                        distance=None,
                        pages=None,
                    )

    # @staticmethod
    # @abstractmethod
    # def search_entities(
    #     session: Session, entity_keys_to_visit: set[tuple[int, EntityType]]
    # ):
    #     pass
    #
    # @abstractmethod
    # def search_via_relational_roles(
    #     self, session: Session, distance, provisional_roles, relations: dict
    # ):
    #     pass

    # PRIVATE METHODS

    def find_clusters(self):
        cluster_count = 0
        cluster_map = {}
        for node in sorted(
            self.nodes.values(),
            key=lambda x: len(x.entity.entities.get("aliases", {})),
            reverse=True,
        ):
            entity = node.entity
            aliases = entity.entities.get("aliases", {})
            if not aliases:
                continue
            if entity.entity_id not in cluster_map:
                cluster_count += 1
                cluster_map[entity.entity_id] = cluster_count
                for _, alias_id in aliases.items():
                    cluster_map[alias_id] = cluster_count
            cluster = cluster_map[entity.entity_id]
            if cluster is not None:
                node.cluster = cluster
        # import pprint
        #
        # log.debug(pprint.pformat(cluster_map))

    @staticmethod
    def page_naively(pages, trellis_nodes_by_distance):
        log.debug("        Paging by naively...")
        index = 0
        for distance in sorted(trellis_nodes_by_distance):
            while trellis_nodes_by_distance[distance]:
                trellis_node = trellis_nodes_by_distance[distance].pop(0)
                pages[index].add(trellis_node)
                index = (index + 1) % len(pages)

    def page_entities(self, pages):
        for page_number, page in enumerate(pages, 1):
            for node in page:
                node.pages.add(page_number)
        grouped_links: Dict[
            Tuple[Tuple[int, EntityType], ...], List[RelationResult]
        ] = {}
        for link in self.links.values():
            key = tuple(sorted([link.entity_one_key, link.entity_two_key]))
            grouped_links[key] = list[RelationResult]()
            grouped_links[key].append(link)
        for (e1k, e2k), links in grouped_links.items():
            entity_one_pages = self.nodes[e1k].pages
            entity_two_pages = self.nodes[e2k].pages
            intersection = entity_one_pages.intersection(entity_two_pages)
            for link in links:
                link.pages = intersection
        for node in self.nodes.values():
            node.missing_by_page.update({page_number: 0 for page_number in node.pages})
            neighbors = node.get_neighbors()
            for neighbor in neighbors:
                for page_number in node.pages.difference(neighbor.pages):
                    node.missing_by_page[page_number] += 1
            if not any(node.missing_by_page.values()):
                node.missing_by_page.clear()

    @staticmethod
    def group_trellis(trellis):
        trellis_nodes_by_distance = collections.OrderedDict()
        for trellis_node in trellis.values():
            if trellis_node.distance not in trellis_nodes_by_distance:
                trellis_nodes_by_distance[trellis_node.distance] = set()
            trellis_nodes_by_distance[trellis_node.distance].add(trellis_node)
        return trellis_nodes_by_distance

    def build_trellis(self):
        for link_key, relation in tuple(self.links.items()):
            if (
                relation.entity_one_key not in self.nodes
                or relation.entity_two_key not in self.nodes
            ):
                self.links.pop(link_key)
                continue
            source_node = self.nodes[relation.entity_one_key]
            source_node.links.add(link_key)
            target_node = self.nodes[relation.entity_two_key]
            target_node.links.add(link_key)
            if source_node.distance == target_node.distance:
                source_node.siblings.add(target_node)
                target_node.siblings.add(source_node)
            elif source_node.distance < target_node.distance:
                source_node.children.add(target_node)
                target_node.parents.add(source_node)
            elif target_node.distance < source_node.distance:
                target_node.children.add(source_node)
                source_node.parents.add(target_node)
        self.recurse_trellis(self.nodes[self.center_entity.entity_key])
        for node_key, node in self.nodes.items():
            if node.subgraph_size is None:
                self.nodes.pop(node_key)
        for link_key, relation in tuple(self.links.items()):
            if (
                relation.entity_one_key not in self.nodes
                or relation.entity_two_key not in self.nodes
            ):
                self.links.pop(link_key)
                continue
        log.debug(
            f"    Built trellis: {len(self.nodes)} nodes / {len(self.links)} links"
        )

    def partition_trellis(self, distance):
        page_count = 1
        # TODO was math.ceil(float(len(self.nodes)) / self.max_nodes)
        log.debug(f"    Partitioning trellis into {page_count} pages...")
        log.debug(f"        Maximum: {self.max_nodes} nodes / {self.max_links} links")
        pages = [set() for _ in range(page_count)]
        trellis_nodes_by_distance = self.group_trellis(self.nodes)
        threshold = len(self.nodes) / len(pages) / len(trellis_nodes_by_distance)
        winning_distance = self.find_trellis_distance(
            trellis_nodes_by_distance,
            threshold,
        )
        self.page_by_local_neighborhood(pages, trellis_nodes_by_distance)
        # TODO: Add fast path when node count is very high (e.g. 4000+)
        if distance > 1:
            self.page_at_winning_distance(
                pages, trellis_nodes_by_distance, winning_distance
            )
            self.page_by_distance(pages, trellis_nodes_by_distance)
        else:
            self.page_naively(pages, trellis_nodes_by_distance)
        for i, page in enumerate(pages):
            log.debug(f"        Page {i}: {len(page)}")
        return pages

    @staticmethod
    def page_at_winning_distance(pages, trellis_nodes_by_distance, winning_distance):
        log.debug("        Paging at winning distance...")
        while trellis_nodes_by_distance[winning_distance]:
            trellis_node = trellis_nodes_by_distance[winning_distance].pop(0)
            parentage = trellis_node.get_parentage()
            pages.sort(
                key=lambda page: (
                    len(page.difference(parentage)),
                    len(page),
                ),
            )
            pages[0].update(parentage)

    # noinspection PyUnusedLocal
    def page_by_local_neighborhood(
        self, pages, trellis_nodes_by_distance, verbose=True
    ):
        local_neighborhood = []
        neighborhood_threshold = len(self.nodes) / len(pages)
        for distance, trellis_nodes in sorted(trellis_nodes_by_distance.items()):
            if len(local_neighborhood) + len(trellis_nodes) < neighborhood_threshold:
                local_neighborhood.extend(trellis_nodes)
                trellis_nodes[:] = []
        log.debug(f"        Paging by local neighborhood: {len(local_neighborhood)}")
        for trellis_node in local_neighborhood:
            parentage = trellis_node.get_parentage()
            for page in pages:
                page.update(parentage)

    @staticmethod
    def page_by_distance(pages, trellis_nodes_by_distance):
        log.debug("        Paging by distance...")
        for distance in sorted(trellis_nodes_by_distance):
            while trellis_nodes_by_distance[distance]:
                trellis_node = trellis_nodes_by_distance[distance].pop(0)
                parentage = trellis_node.get_parentage()
                pages.sort(
                    key=lambda page: (
                        len(page.difference(parentage)),
                        len(page),
                    ),
                )
                pages[0].update(parentage)

    @staticmethod
    def find_trellis_distance(trellis_nodes_by_distance, threshold):
        log.debug(f"        Maximum depth: {max(trellis_nodes_by_distance)}")
        log.debug(f"        Subgraph threshold: {threshold}")
        distancewise_average_subgraph_size = {}
        for distance, trellis_nodes in trellis_nodes_by_distance.items():
            trellis_nodes_by_distance[distance] = sorted(
                trellis_nodes,
                key=lambda x: x.entity_key,
            )
            sizes = sorted(_.subgraph_size for _ in trellis_nodes)
            geometric = sum(sizes) ** (1.0 / len(sizes))
            distancewise_average_subgraph_size[distance] = geometric
            log.debug(f"            At distance {distance}: {geometric} geometric mean")
        winning_distance = 0
        pairs = ((a, d) for d, a in distancewise_average_subgraph_size.items())
        pairs = sorted(pairs, reverse=True)
        for average, distance in pairs:
            log.debug(f"                Testing {average} @ distance {distance}")
            if average < threshold:
                winning_distance = distance
                break
        log.debug(f"            Winning distance: {winning_distance}")
        if (winning_distance + 1) < (len(distancewise_average_subgraph_size) / 2):
            winning_distance += 1
            log.debug(f"            Promoting winning distance: {winning_distance}")
        return winning_distance

    def clear(self):
        self.nodes.clear()
        self.links.clear()
        self.entity_keys_to_visit.clear()
        self.should_break_loop = False

    def prune_roles(self, distance: int, provisional_role_names: List[str]) -> None:
        if distance > 0 and len(self.nodes) > self.max_nodes / 4:
            for role_name in self.roles_to_prune:
                if role_name in provisional_role_names:
                    log.debug(f"            Pruned {role_name} role")
                    provisional_role_names.remove(role_name)
            if self.center_entity.entity_type == EntityType.ARTIST:
                if "Sublabel Of" in provisional_role_names:
                    log.debug(f'            Pruned "Sublabel Of" role')
                    provisional_role_names.remove("Sublabel Of")

    def process_entities(self, distance: int, entities: List[Entity]):
        for entity in sorted(entities, key=lambda x: x.entity_key):
            if not all([entity.entity_id, entity.entity_name]):
                self.entity_keys_to_visit.remove(entity.entity_key)
                continue
            entity_key = entity.entity_key
            if entity_key not in self.nodes:
                # log.debug(f"        add TrellisNode for entity: {entity_key}")
                self.nodes[entity_key] = TrellisNode(entity, distance)

    def process_relations(self, relation_links: Dict[str, RelationResult]) -> None:
        # log.debug(f"    process relations: {relations}")
        for link_key, relation in sorted(relation_links.items()):
            # log.debug(f"        link_key: {link_key}")
            # log.debug(f"        relation: {relation}")

            if not relation.entity_one_id or not relation.entity_two_id:
                # log.debug(f"        skip: {relation}")
                continue
            entity_one_key = relation.entity_one_key
            entity_two_key = relation.entity_two_key
            if entity_one_key not in self.nodes:
                # log.debug(f"        add: {entity_one_key}")
                self.entity_keys_to_visit.add(entity_one_key)
            if entity_two_key not in self.nodes:
                # log.debug(f"        add: {entity_two_key}")
                self.entity_keys_to_visit.add(entity_two_key)
            self.links[link_key] = relation
        # log.debug(f"        entity_keys_to_visit: {self.entity_keys_to_visit}")

    def recurse_trellis(self, node):
        # noinspection PySetFunctionToLiteral
        traversed_keys = set([node.entity_key])
        for child in node.children:
            traversed_keys.update(self.recurse_trellis(child))
        node.subgraph_size = len(traversed_keys)
        # log.debug(f"{'    ' * node.distance}{node.entity.entity_name}: {node.subgraph_size}")
        return traversed_keys

    def report_search_loop_start(self, distance) -> None:
        to_visit_count = len(self.entity_keys_to_visit)
        log.debug(f"    At distance {distance}:")
        log.debug(f"        {len(self.nodes)} old nodes")
        log.debug(f"        {len(self.links)} old links")
        log.debug(f"        {to_visit_count} new nodes")

    def report_search_start(self) -> None:
        log.debug(f"    Max nodes: {self.max_nodes}")
        log.debug(f"    Max links: {self.max_links}")
        log.debug(f"    Roles: {self.all_roles}")

    # noinspection PyUnusedLocal
    def search_via_structural_roles(
        self, distance, provisional_roles, relation_links: Dict[str, RelationResult]
    ) -> None:
        if not self.structural_role_names:
            return
        log.debug("        Retrieving structural relations")
        for entity_key in sorted(self.entity_keys_to_visit):
            # log.debug(f"            entity_key: {entity_key}")
            node = self.nodes.get(entity_key)
            # log.debug(f"            node: {node}")
            if not node:
                # log.debug(f"            ...skipped")
                continue
            entity = node.entity
            # log.debug(f"            entity: {entity}")
            # log.debug(f"            relations: {relations}")
            relation_links.update(
                EntityDataAccess.structural_roles_to_relations(
                    entity, self.structural_role_names
                )
            )

    def test_loop_one(self, distance) -> None:
        if distance > 0:
            if len(self.nodes) >= self.max_nodes:
                log.debug("        Max nodes: exiting next search loop.")
                self.should_break_loop = True

    def test_loop_two(self, distance, relations: dict) -> None:
        if not relations:
            self.should_break_loop = True
        if len(relations) >= self.max_links * 3:
            log.debug("        Max links: exiting next search loop.")
            self.should_break_loop = True
        if distance > 1:
            if len(relations) >= self.max_links:
                log.debug("        Max links: exiting next search loop.")
                self.should_break_loop = True

    # PUBLIC METHODS

    @classmethod
    def make_cache_key(
        cls, template, entity_id: int, entity_type: EntityType, roles=None, year=None
    ) -> str:
        entity_type_str = entity_type.name.lower()
        key = template.format(entity_id=entity_id, entity_type=entity_type_str)
        if roles or year:
            parts = []
            if roles:
                roles = (utils.WORD_PATTERN.sub("+", _) for _ in roles)
                roles = ("roles[]={}".format(_) for _ in roles)
                roles = "&".join(sorted(roles))
                parts.append(roles)
            if year:
                if isinstance(year, int):
                    year = f"year={year}"
                else:
                    year = "-".join(str(_) for _ in year)
                    year = f"year={year}"
                parts.append(year)
            query_string = "&".join(parts)
            key = f"{key}?{query_string}"
        # key = f"discograph:{key}"
        log.debug(f"  cache key: {key}")
        return key

    # PUBLIC PROPERTIES

    @property
    def all_roles(self) -> List[str]:
        return self.structural_role_names + self.relational_role_names

    # @property
    # def should_break_loop(self):
    #     return self.should_break_loop
    #
    # @should_break_loop.setter
    # def should_break_loop(self, expr):
    #     self.should_break_loop = bool(expr)
    #
    # @property
    # def center_entity(self):
    #     return self.center_entity
    #
    # @property
    # def degree(self):
    #     return self.degree
    #
    # @property
    # def entity_keys_to_visit(self):
    #     return self.entity_keys_to_visit
    #
    # @property
    # def link_ratio(self):
    #     return self.link_ratio
    #
    # @property
    # def links(self):
    #     return self.links

    @property
    def max_links(self) -> int:
        return self.max_nodes * self.link_ratio

    # @property
    # def max_nodes(self):
    #     return self.max_nodes
    #
    # @property
    # def nodes(self):
    #     return self.nodes
    #
    # @property
    # def relational_roles(self):
    #     return self.relational_roles
    #
    # @property
    # def structural_roles(self):
    #     return self.structural_roles
