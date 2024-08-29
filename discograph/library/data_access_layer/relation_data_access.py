import itertools
import logging
import re
from typing import List, Dict, Any

from discograph.library.data_access_layer.role_data_access import RoleDataAccess
from discograph.library.domain.release import Release
from discograph.library.fields.entity_type import EntityType
from discograph.library.fields.role_type import RoleType

log = logging.getLogger(__name__)


class RelationDataAccess:

    @classmethod
    def from_release(cls, release: Release) -> List[Dict[str, Any]]:
        # log.debug(f"      release: {release}")
        triples = set()
        artist_pks, label_pks, is_compilation = cls.get_release_setup(release)
        triples.update(
            cls.get_artist_label_relations(
                artist_pks,
                label_pks,
                is_compilation,
            )
        )
        aggregate_roles = {}

        if is_compilation:
            iterator = itertools.product(label_pks, release.extra_artists)
        else:
            iterator = itertools.product(artist_pks, release.extra_artists)
        for entity_two_pk, credit in iterator:
            for roles in credit["roles"]:
                roles_str: str = roles["name"]
                roles_list = RoleDataAccess.normalise_role_names(roles_str)
                for role_str in roles_list:
                    role = cls.normalize_role(role_str)
                    if role in RoleType.aggregate_roles:
                        if role not in aggregate_roles:
                            aggregate_roles[role] = []
                        aggregate_credit = (credit["id"], EntityType.ARTIST)
                        aggregate_roles[role].append(aggregate_credit)
                    elif role in RoleDataAccess.role_name_to_role_id_lookup.keys():
                        entity_one_pk = (credit["id"], EntityType.ARTIST)
                        triples.add((entity_one_pk, role, entity_two_pk))

        if is_compilation:
            iterator = itertools.product(label_pks, release.companies)
        else:
            iterator = itertools.product(artist_pks, release.companies)
        for entity_one_pk, company in iterator:
            roles_str: str = company["entity_type_name"]
            roles_list = RoleDataAccess.normalise_role_names(roles_str)
            for role_str in roles_list:
                role = cls.normalize_role(role_str)
                if role in RoleDataAccess.role_name_to_role_id_lookup.keys():
                    entity_two_pk = (company["id"], EntityType.LABEL)
                    triples.add((entity_one_pk, role, entity_two_pk))

        all_track_artist_pks = set()
        for track in release.tracklist:
            track_artist_pks = set(
                (_["id"], EntityType.ARTIST) for _ in track.get("artists", ())
            )
            all_track_artist_pks.update(track_artist_pks)
            if not track.get("extra_artists"):
                continue
            track_artist_pks = track_artist_pks or artist_pks or label_pks
            iterator = itertools.product(track_artist_pks, track["extra_artists"])
            for entity_two_pk, credit in iterator:
                for roles in credit.get("roles", ()):
                    roles_str: str = roles["name"]
                    roles_list = RoleDataAccess.normalise_role_names(roles_str)
                    for role_str in roles_list:
                        role = cls.normalize_role(role_str)
                        if role in RoleDataAccess.role_name_to_role_id_lookup.keys():
                            entity_one_pk = (credit["id"], EntityType.ARTIST)
                            triples.add((entity_one_pk, role, entity_two_pk))
        for role, aggregate_artists in aggregate_roles.items():
            iterator = itertools.product(all_track_artist_pks, aggregate_artists)
            for track_artist_pk, aggregate_artist_pk in iterator:
                entity_one_pk = aggregate_artist_pk
                entity_two_pk = track_artist_pk
                triples.add((entity_one_pk, role, entity_two_pk))
        # log.debug(f"triples3: {triples}")
        triples = sorted(triples)
        # log.debug(f"      triples: {triples}")
        relations = cls.from_triples(triples, release=release)
        # log.debug(f"      relations: {relations}")
        return relations

    @classmethod
    def get_artist_label_relations(
        cls,
        artist_pks: set[tuple[int, EntityType]],
        label_pks: set[tuple[int, EntityType]],
        is_compilation: bool,
    ):
        triples = set()
        iterator = itertools.product(artist_pks, label_pks)
        if is_compilation:
            role = "Compiled On"
        else:
            role = "Released On"
        for artist_pk, label_pk in iterator:
            triples.add((artist_pk, role, label_pk))
        return triples

    @classmethod
    def get_release_setup(
        cls, release
    ) -> tuple[set[tuple[int, EntityType]], set[tuple[int, EntityType]], bool]:
        is_compilation = False
        # log.debug(f"get_release_setup release: {release}")
        artist_pks: set[tuple[int, EntityType]] = set(
            (_["id"], EntityType.ARTIST) for _ in release.artists
        )
        # log.debug(f"get_release_setup artists: {artist_pks}")
        label_pks: set[tuple[int, EntityType]] = set(
            (_.get("id"), EntityType.LABEL) for _ in release.labels if _.get("id")
        )
        # log.debug(f"get_release_setup labels: {label_pks}")
        if len(artist_pks) == 1 and release.artists[0]["name"] == "Various":
            is_compilation = True
            artist_pks.clear()
            for track in release.tracklist:
                artist_pks.update(
                    (_["id"], EntityType.ARTIST) for _ in track.get("artists", ())
                )
        # for format_ in release.formats:
        #    for description in format_.get('descriptions', ()):
        #        if description == 'Compilation':
        #            is_compilation = True
        #            break
        return artist_pks, label_pks, is_compilation

    @classmethod
    def from_triples(cls, triples, release=None) -> List[Dict[str, Any]]:
        relations = []
        for entity_one_pk, role, entity_two_pk in triples:
            entity_one_id, entity_one_type = entity_one_pk
            entity_two_id, entity_two_type = entity_two_pk
            relation = dict(
                entity_one_id=entity_one_id,
                entity_one_type=entity_one_type,
                entity_two_id=entity_two_id,
                entity_two_type=entity_two_type,
                role=role,
            )
            if release is not None:
                relation["release_id"] = release.release_id
                if release.release_date is not None:
                    relation["year"] = release.release_date.year
            relations.append(relation)
        return relations

    @staticmethod
    def normalize_role(role: str) -> str:
        if role in RoleType.aggregate_roles:
            return role
        if role in RoleDataAccess.role_name_to_role_id_lookup.keys():
            return role
        # log.debug(f"role not found: {role}")

        normalized_role = RoleDataAccess.normalise_role_name(role)
        if normalized_role in RoleType.aggregate_roles:
            return normalized_role
        if normalized_role in RoleDataAccess.role_name_to_role_id_lookup.keys():
            return normalized_role

        name = normalized_role
        name = re.sub(r"(\s|-)+(BY|By|by)($|\s)", "", name)
        name = re.sub(r"(\s|-)+(TO|To|to)($|\s)", "", name)
        name = re.sub(r"(\s|-)+(AT|At|at)($|\s)", "", name)
        name = re.sub(r"(\s|-)+(FOR|For|for)($|\s)", "", name)
        name = re.sub(r"(\s|-)+(ON|On|on)($|\s)", "", name)
        name = re.sub(r"(\s|-)+(WITH|With|with)($|\s)", "", name)
        if name in RoleType.aggregate_roles:
            return name
        if name in RoleDataAccess.role_name_to_role_id_lookup.keys():
            return name

        role_by = normalized_role + " By"
        if role_by in RoleType.aggregate_roles:
            return role_by
        if role_by in RoleDataAccess.role_name_to_role_id_lookup.keys():
            return role_by

        role_to = normalized_role + " To"
        if role_to in RoleType.aggregate_roles:
            return role_to
        if role_to in RoleDataAccess.role_name_to_role_id_lookup.keys():
            return role_to

        log.debug(f"role not found: {role}")
        return role
