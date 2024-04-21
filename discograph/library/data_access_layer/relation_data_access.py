import itertools
import logging
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
            for role in credit["roles"]:
                role = role["name"]
                if role not in RoleType.role_definitions:
                    # log.debug(f"role not found: {role}")
                    role = RoleDataAccess.normalize(role)
                if role not in RoleType.role_definitions:
                    log.debug(f"normalized role not found: {role}")
                    continue
                elif role in RoleType.aggregate_roles:
                    if role not in aggregate_roles:
                        aggregate_roles[role] = []
                    aggregate_credit = (credit["id"], EntityType.ARTIST)
                    aggregate_roles[role].append(aggregate_credit)
                    continue
                entity_one_pk = (credit["id"], EntityType.ARTIST)
                triples.add((entity_one_pk, role, entity_two_pk))

        if is_compilation:
            iterator = itertools.product(label_pks, release.companies)
        else:
            iterator = itertools.product(artist_pks, release.companies)
        for entity_one_pk, company in iterator:
            role = company["entity_type_name"]
            if role not in RoleType.role_definitions:
                # log.debug(f"role not found: {role}")
                role = RoleDataAccess.normalize(role)
            if role not in RoleType.role_definitions:
                log.debug(f"normalized role not found: {role}")
                continue
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
                for role in credit.get("roles", ()):
                    role = role["name"]
                    if role not in RoleType.role_definitions:
                        # log.debug(f"role not found: {role}")
                        role = RoleDataAccess.normalize(role)
                    if role not in RoleType.role_definitions:
                        log.debug(f"normalized role not found: {role}")
                        continue
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
