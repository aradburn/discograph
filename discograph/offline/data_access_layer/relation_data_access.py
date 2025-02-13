import itertools
import logging
from typing import List, Dict, Any

from discograph.library.fields.role_type import RoleType
from discograph.offline.data_access_layer.role_data_access import RoleDataAccess
from discograph.offline.database.entity_repository import EntityRepository
from discograph.offline.database.relation_repository import RelationRepository
from discograph.offline.domain.relation import Relation, RelationInternal
from discograph.offline.domain.release import Release

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
                            if "id" in credit:
                                aggregate_credit_id = credit["id"]
                                aggregate_roles[role_name].append(aggregate_credit_id)
                        else:
                            if "id" in credit:
                                triples.add((credit["id"], role_name, object_id))
                            # TODO find missing id
                            # else:
                            # entity_type = EntityType.ARTIST
                            # entity_id = entry["id"]
                            # id_ = EntityDataAccess.get_internal_id_by_entity_type_and_entity_id(
                            #     entity_repository, entity_type, entity_id
                            # )

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
                    if "id" in company:
                        triples.add(
                            (
                                subject_id,
                                role_name,
                                company["id"],
                            )
                        )

        all_track_artist_ids = set()
        for track in release.tracklist:
            track_artist_ids = set(
                artist["id"] for artist in track.get("artists", ()) if "id" in artist
            )
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
                            if "id" in credit:
                                subject_id = credit["id"]
                                triples.add((subject_id, role_name, object_id))
                            # TODO find missing id
                            # else:
                            # entity_type = EntityType.ARTIST
                            # entity_id = entry["id"]
                            # id_ = EntityDataAccess.get_internal_id_by_entity_type_and_entity_id(
                            #     entity_repository, entity_type, entity_id
                            # )
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
        artist_ids: set[int] = set(
            artist["id"] for artist in release.artists if "id" in artist
        )
        # log.debug(f"get_release_setup artists: {artist_ids}")
        label_ids: set[int] = set(
            label["id"] for label in release.labels if "id" in label
        )
        # log.debug(f"get_release_setup labels: {label_ids}")

        if len(artist_ids) == 1 and release.artists[0]["name"] == "Various":
            is_compilation = True
            artist_ids.clear()
            for track in release.tracklist:
                artist_ids.update(
                    artist["id"]
                    for artist in track.get("artists", ())
                    if "id" in artist
                )
            # log.debug(f"get_release_setup various artists: {artist_ids}")

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
    def find_relation_by_key(
        cls,
        *,
        entity_repository: EntityRepository,
        relation_repository: RelationRepository,
        key: dict[str, Any],
    ) -> list[Relation]:
        pass

    # @classmethod
    # def to_relation_external_id(cls, id: int) -> int:
    #     if entity_type == EntityType.ARTIST:
    #         return entity_id
    #     else:
    #         return entity_id + EntityDataAccess.LABEL_ENTITY_ID_OFFSET

    @classmethod
    def relation_internal_dict_to_relation_external_dict(
        cls,
        relation_internal_dict: dict[str, Any],
    ) -> dict[str, Any] | None:
        relation_internal_dict["id"] = 0
        relation_internal = RelationInternal.model_validate(relation_internal_dict)
        relation = relation_internal.to_relation()
        if relation is None:
            return None
        relation_external_dict = relation.model_dump(exclude={"id", "releases"})
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
