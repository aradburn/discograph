import itertools
import logging
from typing import List, Dict, Any

from discograph.exceptions import NotFoundError
from discograph.library.data_access_layer.entity_data_access import EntityDataAccess
from discograph.library.data_access_layer.role_data_access import RoleDataAccess
from discograph.library.database.entity_repository import EntityRepository
from discograph.library.database.relation_repository import RelationRepository
from discograph.library.domain.relation import Relation, RelationInternal
from discograph.library.domain.release import Release
from discograph.library.fields.entity_type import EntityType
from discograph.library.fields.role_type import RoleType

log = logging.getLogger(__name__)


class RelationDataAccess:

    @classmethod
    def from_release(cls, release: Release) -> List[Dict[str, Any]]:
        # log.debug(f"      release: {release}")
        triples = set()
        artist_ids, label_ids, is_compilation = cls.get_release_setup(release)

        triples.update(
            cls.get_artist_label_relations(
                artist_ids,
                label_ids,
                is_compilation,
            )
        )
        aggregate_roles = {}

        if is_compilation:
            iterator = itertools.product(label_ids, release.extra_artists)
        else:
            iterator = itertools.product(artist_ids, release.extra_artists)
        for object_id, credit in iterator:
            for roles in credit["roles"]:
                input_role_str: str = roles["name"]
                role_str_list = RoleDataAccess.normalise_role_names(input_role_str)
                for role_str in role_str_list:
                    role_name = RoleDataAccess.find_role(role_str)
                    if role_name is not None:
                        if role_name in RoleType.aggregate_roles:
                            if role_name not in aggregate_roles:
                                aggregate_roles[role_name] = []
                            aggregate_credit_id = credit["id"]
                            aggregate_roles[role_name].append(aggregate_credit_id)
                        else:
                            triples.add((credit["id"], role_name, object_id))

        if is_compilation:
            iterator = itertools.product(label_ids, release.companies)
        else:
            iterator = itertools.product(artist_ids, release.companies)
        for subject_id, company in iterator:
            input_role_str: str = company["entity_type_name"]
            role_strs_list = RoleDataAccess.normalise_role_names(input_role_str)
            for role_str in role_strs_list:
                role_name = RoleDataAccess.find_role(role_str)
                if role_name is not None:
                    triples.add(
                        (
                            subject_id,
                            role_name,
                            company["id"] + EntityDataAccess.LABEL_ENTITY_ID_OFFSET,
                        )
                    )

        all_track_artist_ids = set()
        for track in release.tracklist:
            track_artist_ids = set(artist["id"] for artist in track.get("artists", ()))
            all_track_artist_ids.update(track_artist_ids)
            if not track.get("extra_artists"):
                continue
            track_artist_ids = track_artist_ids or artist_ids or label_ids
            iterator = itertools.product(track_artist_ids, track["extra_artists"])
            for object_id, credit in iterator:
                for roles in credit.get("roles", ()):
                    input_role_str: str = roles["name"]
                    role_strs_list = RoleDataAccess.normalise_role_names(input_role_str)
                    for role_str in role_strs_list:
                        role_name = RoleDataAccess.find_role(role_str)
                        if role_name is not None:
                            subject_id = credit["id"]
                            triples.add((subject_id, role_name, object_id))
        for role_name, aggregate_artists in aggregate_roles.items():
            iterator = itertools.product(all_track_artist_ids, aggregate_artists)
            for track_artist_id, aggregate_artist_id in iterator:
                subject_id = aggregate_artist_id
                object_id = track_artist_id
                triples.add((subject_id, role_name, object_id))
        # log.debug(f"triples3: {triples}")
        triples = sorted(triples)
        # log.debug(f"      triples: {triples}")
        relations = cls.from_triples(triples, release=release)
        # log.debug(f"      relations: {relations}")
        return relations

    @classmethod
    def get_artist_label_relations(
        cls,
        artist_ids: set[int],
        label_ids: set[int],
        is_compilation: bool,
    ) -> set[tuple[int, str, int]]:
        # print(f"artist_ids: {artist_ids}")
        # print(f"label_ids: {label_ids}")
        triples = set()
        iterator = itertools.product(artist_ids, label_ids)
        if is_compilation:
            role = "Compiled On"
        else:
            role = "Released On"
        for artist_id, label_id in iterator:
            triples.add((artist_id, role, label_id))
        return triples

    @classmethod
    def get_release_setup(cls, release) -> tuple[set[int], set[int], bool]:
        is_compilation = False
        # log.debug(f"get_release_setup release: {release}")
        artist_ids: set[int] = set(artist["id"] for artist in release.artists)
        # log.debug(f"get_release_setup artists: {artist_pks}")
        label_ids: set[int] = set()
        for label in release.labels:
            label_id = label.get("id")
            if label_id:
                if label_id != EntityDataAccess.MISSING_LABEL_ENTITY:
                    label_ids.add(label_id + EntityDataAccess.LABEL_ENTITY_ID_OFFSET)
                else:
                    label_ids.add(label_id)

        # log.debug(f"get_release_setup labels: {label_pks}")
        if len(artist_ids) == 1 and release.artists[0]["name"] == "Various":
            is_compilation = True
            artist_ids.clear()
            for track in release.tracklist:
                artist_ids.update(artist["id"] for artist in track.get("artists", ()))

        # for format_ in release.formats:
        #    for description in format_.get('descriptions', ()):
        #        if description == 'Compilation':
        #            is_compilation = True
        #            break
        return artist_ids, label_ids, is_compilation

    # @classmethod
    # def get_release_setup(
    #     cls, release
    # ) -> tuple[set[tuple[int, EntityType]], set[tuple[int, EntityType]], bool]:
    #     is_compilation = False
    #     # log.debug(f"get_release_setup release: {release}")
    #     artist_pks: set[tuple[int, EntityType]] = set(
    #         (_["id"], EntityType.ARTIST) for _ in release.artists
    #     )
    #     # log.debug(f"get_release_setup artists: {artist_pks}")
    #     label_pks: set[tuple[int, EntityType]] = set(
    #         (_.get("id"), EntityType.LABEL) for _ in release.labels if _.get("id")
    #     )
    #     # log.debug(f"get_release_setup labels: {label_pks}")
    #     if len(artist_pks) == 1 and release.artists[0]["name"] == "Various":
    #         is_compilation = True
    #         artist_pks.clear()
    #         for track in release.tracklist:
    #             artist_pks.update(
    #                 (_["id"], EntityType.ARTIST) for _ in track.get("artists", ())
    #             )
    #     # for format_ in release.formats:
    #     #    for description in format_.get('descriptions', ()):
    #     #        if description == 'Compilation':
    #     #            is_compilation = True
    #     #            break
    #     return artist_pks, label_pks, is_compilation

    @classmethod
    def from_triples(cls, triples, release=None) -> List[Dict[str, Any]]:
        relations = []
        for subject_id, role, object_id in triples:
            relation = dict(
                subject=subject_id,
                role=role,
                object=object_id,
            )
            if release is not None:
                relation["release_id"] = release.release_id
                if release.release_date is not None:
                    relation["year"] = release.release_date.year
            relations.append(relation)
        return relations

    @classmethod
    def search_multi(
        cls,
        *,
        entity_repository: EntityRepository,
        relation_repository: RelationRepository,
        entity_keys: list[tuple[int, EntityType]],
        role_names: list[str],
    ) -> list[Relation]:
        assert entity_keys
        assert role_names

        relation_internals = []

        role_ids = [
            RoleDataAccess.role_name_to_role_id_lookup[role_name]
            for role_name in role_names
        ]

        for entity_id, entity_type in entity_keys:
            entity = entity_repository.get_by_entity_id_and_entity_type(
                entity_id, entity_type
            )
            if entity:
                entity_relations = relation_repository.find_by_entity_and_roles(
                    entity.id, role_ids
                )
                relation_internals.extend(entity_relations)

        relations = [
            RelationDataAccess.to_relation(relation_internal)
            for relation_internal in relation_internals
        ]
        return relations

    # def search_bimulti(
    #     self,
    #     lh_entities: list[tuple[int, EntityType]],
    #     rh_entities: list[tuple[int, EntityType]],
    #     role_names: list[str] = None,
    #     year=None,
    #     verbose=True,
    # ) -> List[Relation]:
    #     lh_artist_ids = []
    #     lh_label_ids = []
    #     rh_artist_ids = []
    #     rh_label_ids = []
    #     for entity_id, entity_type in lh_entities:
    #         if entity_type == EntityType.ARTIST:
    #             lh_artist_ids.append(entity_id)
    #         else:
    #             lh_label_ids.append(entity_id)
    #     for entity_id, entity_type in rh_entities:
    #         if entity_type == EntityType.ARTIST:
    #             rh_artist_ids.append(entity_id)
    #         else:
    #             rh_label_ids.append(entity_id)
    #     relations: List[Relation] = []
    #     if lh_artist_ids:
    #         lh_type = EntityType.ARTIST
    #         lh_ids = lh_artist_ids
    #         if rh_artist_ids:
    #             rh_type = EntityType.ARTIST
    #             rh_ids = rh_artist_ids
    #             results1 = self.find_by_type_and_ids_and_role_names(
    #                 lh_type, lh_ids, rh_type, rh_ids, role_names
    #             )
    #             relations.extend(results1)
    #         if rh_label_ids:
    #             rh_type = EntityType.LABEL
    #             rh_ids = rh_label_ids
    #             results2 = self.find_by_type_and_ids_and_role_names(
    #                 lh_type, lh_ids, rh_type, rh_ids, role_names
    #             )
    #             relations.extend(results2)
    #     if lh_label_ids:
    #         lh_type = EntityType.LABEL
    #         lh_ids = lh_label_ids
    #         if rh_artist_ids:
    #             rh_type = EntityType.ARTIST
    #             rh_ids = rh_artist_ids
    #             results3 = self.find_by_type_and_ids_and_role_names(
    #                 lh_type, lh_ids, rh_type, rh_ids, role_names
    #             )
    #             relations.extend(results3)
    #         if rh_label_ids:
    #             rh_type = EntityType.LABEL
    #             rh_ids = rh_label_ids
    #             results4 = self.find_by_type_and_ids_and_role_names(
    #                 lh_type, lh_ids, rh_type, rh_ids, role_names
    #             )
    #             relations.extend(results4)
    #     return relations
    #     # for query in queries:
    #     #     log.debug(f"search_bimulti query: {query}")
    #     #     relations.extend(query)
    #     # relation_links = {relation.link_key: relation for relation in relations}
    #     # log.debug(f"relation_links: {relation_links}")
    #     # return relation_links

    @classmethod
    def find_relation_by_key(
        cls,
        *,
        entity_repository: EntityRepository,
        relation_repository: RelationRepository,
        key: dict[str, Any],
    ) -> list[Relation]:
        pass

    @classmethod
    def to_relation_internal_id(cls, entity_id: int, entity_type: EntityType) -> int:
        if entity_type == EntityType.ARTIST:
            return entity_id
        else:
            return entity_id + EntityDataAccess.LABEL_ENTITY_ID_OFFSET

    # @classmethod
    # def to_relation_external_id(cls, id: int) -> int:
    #     if entity_type == EntityType.ARTIST:
    #         return entity_id
    #     else:
    #         return entity_id + EntityDataAccess.LABEL_ENTITY_ID_OFFSET

    @classmethod
    def to_relation(cls, relation_internal: RelationInternal) -> Relation | None:
        try:
            if relation_internal.subject == EntityDataAccess.MISSING_LABEL_ENTITY:
                entity_one_id = -1
                entity_one_type = EntityType.LABEL
            elif relation_internal.subject >= EntityDataAccess.LABEL_ENTITY_ID_OFFSET:
                entity_one_id = (
                    relation_internal.subject - EntityDataAccess.LABEL_ENTITY_ID_OFFSET
                )
                entity_one_type = EntityType.LABEL
            else:
                entity_one_id = relation_internal.subject
                entity_one_type = EntityType.ARTIST
            if relation_internal.object == EntityDataAccess.MISSING_LABEL_ENTITY:
                entity_two_id = -1
                entity_two_type = EntityType.LABEL
            elif relation_internal.object >= EntityDataAccess.LABEL_ENTITY_ID_OFFSET:
                entity_two_id = (
                    relation_internal.object - EntityDataAccess.LABEL_ENTITY_ID_OFFSET
                )
                entity_two_type = EntityType.LABEL
            else:
                entity_two_id = relation_internal.object
                entity_two_type = EntityType.ARTIST
            return Relation(
                id=relation_internal.id,
                entity_one_id=entity_one_id,
                entity_one_type=entity_one_type,
                entity_two_id=entity_two_id,
                entity_two_type=entity_two_type,
                role=relation_internal.role,
                random=relation_internal.random,
            )
        except NotFoundError:
            return None

    @classmethod
    def to_relations(
        cls,
        relation_internals: list[RelationInternal],
    ) -> list[Relation]:
        relations = []
        for relation_internal in relation_internals:
            relation = RelationDataAccess.to_relation(relation_internal)
            if relation:
                relations.append(relation)
        return relations

    @classmethod
    def relation_internal_dict_to_relation_external_dict(
        cls,
        relation_internal_dict: dict[str, Any],
    ) -> dict[str, Any] | None:
        relation_internal_dict["id"] = 0
        relation_internal_dict["random"] = 0
        relation_internal = RelationInternal.model_validate(relation_internal_dict)
        relation = RelationDataAccess.to_relation(relation_internal)
        if relation is None:
            return None
        relation_external_dict = relation.model_dump(
            exclude={"id", "random", "releases"}
        )
        relation_external_dict["release_id"] = relation_internal_dict["release_id"]
        relation_external_dict["year"] = relation_internal_dict["year"]
        return relation_external_dict

    @classmethod
    def relation_internal_dicts_to_relation_external_dicts(
        cls,
        relation_internal_dicts: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        relation_external_dicts = []
        for relation_internal_dict in relation_internal_dicts:
            relation_external_dict = (
                RelationDataAccess.relation_internal_dict_to_relation_external_dict(
                    relation_internal_dict
                )
            )
            if relation_external_dict:
                relation_external_dicts.append(relation_external_dict)
        return relation_external_dicts
