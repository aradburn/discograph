import collections
import collections.abc as collections_abc
import logging
import re
from abc import abstractmethod, ABC

from discograph.library import CreditRole, EntityType
from discograph.library.database_helper import DatabaseHelper
from discograph.library.trellis_node import TrellisNode

log = logging.getLogger(__name__)


class RelationGrapher(ABC):
    # CLASS VARIABLES

    __slots__ = (
        "_should_break_loop",
        "_center_entity",
        "_degree",
        "_entity_keys_to_visit",
        "_link_ratio",
        "_links",
        "_max_nodes",
        "_nodes",
        "_relational_roles",
        "_structural_roles",
    )

    roles_to_prune = [
        "Released On",
        "Compiled On",
        "Producer",
        "Remix",
        "DJ Mix",
        "Written-By",
    ]

    word_pattern = re.compile(r"\s+")

    # INITIALIZER

    def __init__(
        self,
        center_entity,
        degree=DatabaseHelper.MAX_DEGREE,
        link_ratio=None,
        max_nodes=None,
        roles=None,
    ):
        log.debug(f"RelationGrapher for {center_entity.name}")
        self._center_entity = center_entity
        degree = int(degree)
        assert degree > 0
        self._degree = degree
        if max_nodes is not None:
            max_nodes = int(max_nodes)
            assert max_nodes > 0
        else:
            max_nodes = DatabaseHelper.MAX_NODES
        self._max_nodes = max_nodes
        if link_ratio is not None:
            link_ratio = int(link_ratio)
            assert link_ratio > 0
        else:
            link_ratio = DatabaseHelper.LINK_RATIO
        self._link_ratio = link_ratio
        roles = roles or ()
        structural_roles, relational_roles = [], []
        if roles:
            if isinstance(roles, str):
                roles = (roles,)
            elif not isinstance(roles, collections_abc.Iterable):
                roles = (roles,)
            roles = tuple(roles)
            assert all(_ in CreditRole.all_credit_roles for _ in roles)
            for role in roles:
                if role in ("Alias", "Sublabel Of", "Member Of"):
                    structural_roles.append(role)
                else:
                    relational_roles.append(role)
        self._structural_roles = tuple(structural_roles)
        self._relational_roles = tuple(relational_roles)
        self._nodes = collections.OrderedDict()
        self._links = {}
        self._should_break_loop = False
        self._entity_keys_to_visit = set()

    def __call__(self):
        log.debug(f"Searching around {self.center_entity.name}...")
        provisional_roles = list(self.relational_roles)
        self._report_search_start()
        self._clear()
        self.entity_keys_to_visit.add(self.center_entity.entity_key)
        distance = 0
        for distance in range(self.degree + 1):
            self._report_search_loop_start(distance)
            log.debug(f"    Search for: {self.entity_keys_to_visit}")
            entities = self._search_entities(self.entity_keys_to_visit)
            # log.debug(f"    Search found entities: {entities}")
            relations = {}
            self._process_entities(distance, entities)
            if not self.entity_keys_to_visit or self.should_break_loop:
                break
            self._test_loop_one(distance)
            self._prune_roles(distance, provisional_roles)
            if not self.should_break_loop:
                self._search_via_structural_roles(
                    distance, provisional_roles, relations
                )
                self._search_via_relational_roles(
                    distance, provisional_roles, relations
                )
            self._test_loop_two(distance, relations)
            self.entity_keys_to_visit.clear()
            self._process_relations(relations)
        self._build_trellis()
        # self._cross_reference(distance)
        pages = self._partition_trellis(distance)
        self._page_entities(pages)
        self._find_clusters()
        for node in self.nodes.values():
            expected_count = node.entity.roles_to_relation_count(self.all_roles)
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
                "name": self.center_entity.name,
            },
            "links": json_links,
            "nodes": json_nodes,
            "pages": len(pages),
        }
        return network

    @staticmethod
    @abstractmethod
    def _search_entities(entity_keys_to_visit):
        pass

    @abstractmethod
    def _search_via_relational_roles(self, distance, provisional_roles, relations):
        pass

    # PRIVATE METHODS

    def _find_clusters(self):
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
    def _page_naively(pages, trellis_nodes_by_distance):
        log.debug("        Paging by naively...")
        index = 0
        for distance in sorted(trellis_nodes_by_distance):
            while trellis_nodes_by_distance[distance]:
                trellis_node = trellis_nodes_by_distance[distance].pop(0)
                pages[index].add(trellis_node)
                index = (index + 1) % len(pages)

    def _page_entities(self, pages):
        for page_number, page in enumerate(pages, 1):
            for node in page:
                node.pages.add(page_number)
        grouped_links = {}
        for link in self.links.values():
            key = tuple(sorted([link.entity_one_key, link.entity_two_key]))
            grouped_links.setdefault(key, [])
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
    def _group_trellis(trellis):
        trellis_nodes_by_distance = collections.OrderedDict()
        for trellis_node in trellis.values():
            if trellis_node.distance not in trellis_nodes_by_distance:
                trellis_nodes_by_distance[trellis_node.distance] = set()
            trellis_nodes_by_distance[trellis_node.distance].add(trellis_node)
        return trellis_nodes_by_distance

    def _build_trellis(self):
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
        self._recurse_trellis(self.nodes[self.center_entity.entity_key])
        for node_key, node in tuple(self.nodes.items()):
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

    def _partition_trellis(self, distance):
        page_count = 1
        # TODO was math.ceil(float(len(self.nodes)) / self.max_nodes)
        log.debug(f"    Partitioning trellis into {page_count} pages...")
        log.debug(f"        Maximum: {self.max_nodes} nodes / {self.max_links} links")
        pages = [set() for _ in range(page_count)]
        trellis_nodes_by_distance = self._group_trellis(self.nodes)
        threshold = len(self.nodes) / len(pages) / len(trellis_nodes_by_distance)
        winning_distance = self._find_trellis_distance(
            trellis_nodes_by_distance,
            threshold,
        )
        self._page_by_local_neighborhood(pages, trellis_nodes_by_distance)
        # TODO: Add fast path when node count is very high (e.g. 4000+)
        if distance > 1:
            self._page_at_winning_distance(
                pages, trellis_nodes_by_distance, winning_distance
            )
            self._page_by_distance(pages, trellis_nodes_by_distance)
        else:
            self._page_naively(pages, trellis_nodes_by_distance)
        for i, page in enumerate(pages):
            log.debug(f"        Page {i}: {len(page)}")
        return pages

    @staticmethod
    def _page_at_winning_distance(pages, trellis_nodes_by_distance, winning_distance):
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
    def _page_by_local_neighborhood(
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
    def _page_by_distance(pages, trellis_nodes_by_distance):
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
    def _find_trellis_distance(trellis_nodes_by_distance, threshold):
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

    def _clear(self):
        self._nodes.clear()
        self._links.clear()
        self._entity_keys_to_visit.clear()
        self._should_break_loop = False

    def _prune_roles(self, distance, provisional_roles):
        if distance > 0 and len(self.nodes) > self.max_nodes / 4:
            for role in self.roles_to_prune:
                if role in provisional_roles:
                    log.debug("            Pruned {!r} role".format(role))
                    provisional_roles.remove(role)
            if self.center_entity.entity_type == EntityType.ARTIST:
                if "Sublabel Of" in provisional_roles:
                    log.debug("            Pruned {!r} role".format("Sublabel Of"))
                    provisional_roles.remove("Sublabel Of")

    def _process_entities(self, distance, entities):
        for entity in sorted(entities, key=lambda x: x.entity_key):
            if not all([entity.entity_id, entity.name]):
                self.entity_keys_to_visit.remove(entity.entity_key)
                continue
            entity_key = entity.entity_key
            if entity_key not in self.nodes:
                # log.debug(f"        add TrellisNode for entity: {entity_key}")
                self.nodes[entity_key] = TrellisNode(entity, distance)

    def _process_relations(self, relations):
        # log.debug(f"    process relations: {relations}")
        for link_key, relation in sorted(relations.items()):
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

    def _recurse_trellis(self, node):
        # noinspection PySetFunctionToLiteral
        traversed_keys = set([node.entity_key])
        for child in node.children:
            traversed_keys.update(self._recurse_trellis(child))
        node.subgraph_size = len(traversed_keys)
        # log.debug(f"{'    ' * node.distance}{node.entity.name}: {node.subgraph_size}")
        return traversed_keys

    def _report_search_loop_start(self, distance):
        to_visit_count = len(self.entity_keys_to_visit)
        log.debug(f"    At distance {distance}:")
        log.debug(f"        {len(self.nodes)} old nodes")
        log.debug(f"        {len(self.links)} old links")
        log.debug(f"        {to_visit_count} new nodes")

    def _report_search_start(self):
        log.debug(f"    Max nodes: {self.max_nodes}")
        log.debug(f"    Max links: {self.max_links}")
        log.debug(f"    Roles: {self.all_roles}")

    # noinspection PyUnusedLocal
    def _search_via_structural_roles(self, distance, provisional_roles, relations):
        if not self.structural_roles:
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
            relations.update(
                entity.structural_roles_to_relations(self.structural_roles)
            )

    def _test_loop_one(self, distance):
        if distance > 0:
            if len(self.nodes) >= self.max_nodes:
                log.debug("        Max nodes: exiting next search loop.")
                self.should_break_loop = True

    def _test_loop_two(self, distance, relations):
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
        cls, template, entity_type: EntityType, entity_id, roles=None, year=None
    ):
        assert entity_type in (EntityType.ARTIST, EntityType.LABEL)
        # if isinstance(entity_type, int):
        #     entity_type = cls.entity_type_names[entity_type]
        entity_type_str = entity_type.name.lower()
        key = template.format(entity_type=entity_type_str, entity_id=entity_id)
        if roles or year:
            parts = []
            if roles:
                roles = (cls.word_pattern.sub("+", _) for _ in roles)
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
        key = f"discograph:{key}"
        log.debug(f"  cache key: {key}")
        return key

    # PUBLIC PROPERTIES

    @property
    def all_roles(self):
        return self.structural_roles + self.relational_roles

    @property
    def should_break_loop(self):
        return self._should_break_loop

    @should_break_loop.setter
    def should_break_loop(self, expr):
        self._should_break_loop = bool(expr)

    @property
    def center_entity(self):
        return self._center_entity

    @property
    def degree(self):
        return self._degree

    @property
    def entity_keys_to_visit(self):
        return self._entity_keys_to_visit

    @property
    def link_ratio(self):
        return self._link_ratio

    @property
    def links(self):
        return self._links

    @property
    def max_links(self):
        return self._max_nodes * self._link_ratio

    @property
    def max_nodes(self):
        return self._max_nodes

    @property
    def nodes(self):
        return self._nodes

    @property
    def relational_roles(self):
        return self._relational_roles

    @property
    def structural_roles(self):
        return self._structural_roles
