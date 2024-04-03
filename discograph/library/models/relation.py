import itertools
import logging
import multiprocessing
import random
import re
import sys
import time
from typing import cast

from sqlalchemy import (
    PrimaryKeyConstraint,
    JSON,
    String,
    Float,
    select,
    Index,
)
from sqlalchemy.exc import DatabaseError, NoResultFound, IntegrityError
from sqlalchemy.orm import Mapped, mapped_column, scoped_session, Session
from sqlalchemy.orm.attributes import flag_modified

from discograph.library.database.database_helper import Base, DatabaseHelper
from discograph.library.fields.entity_type import EntityType
from discograph.library.fields.int_enum import IntEnum
from discograph.library.fields.role_type import RoleType
from discograph.library.loader_base import LoaderBase
from discograph.library.models.role import Role
from discograph.logging_config import LOGGING_TRACE

log = logging.getLogger(__name__)


class Relation(Base, LoaderBase):
    __tablename__ = "relation"

    # COLUMNS

    entity_one_id: Mapped[int] = mapped_column(primary_key=True)
    entity_one_type: Mapped[EntityType] = mapped_column(
        IntEnum(EntityType),
        primary_key=True,
    )
    entity_two_id: Mapped[int] = mapped_column(primary_key=True)
    entity_two_type: Mapped[EntityType] = mapped_column(
        IntEnum(EntityType),
        primary_key=True,
    )
    role: Mapped[str] = mapped_column(String, primary_key=True)
    releases: Mapped[dict | list] = mapped_column(type_=JSON, nullable=False)
    random: Mapped[float] = mapped_column(Float)

    __table_args__ = (
        PrimaryKeyConstraint(
            entity_one_id, entity_one_type, entity_two_id, entity_two_type, role
        ),
        Index(
            "idx_entity_one_id",
            entity_one_id,
            postgresql_using="hash",
            unique=False,
        ),
        Index(
            "idx_entity_two_id",
            entity_two_id,
            postgresql_using="hash",
            unique=False,
        ),
        {},
    )

    # CLASS VARIABLES

    class LoaderPassOneWorker(multiprocessing.Process):
        corpus = {}

        def __init__(self, model_class, indices):
            super().__init__()
            self.model_class = model_class
            self.indices = indices

        def run(self):
            proc_name = self.name
            relation_class_name = self.model_class.__qualname__
            relation_module_name = self.model_class.__module__
            release_class_name = relation_class_name.replace("Relation", "Release")
            release_module_name = relation_module_name.replace("relation", "release")
            release_class = getattr(
                sys.modules[release_module_name], release_class_name
            )

            count = 0
            total_count = len(self.indices)

            DatabaseHelper.initialize()
            worker_session = scoped_session(DatabaseHelper.session_factory)

            with worker_session() as session:
                for release_id in self.indices:
                    max_attempts = 100
                    error = True
                    while error and max_attempts != 0:
                        error = False
                        session.begin()
                        try:
                            self.model_class.loader_pass_one_inner(
                                session,
                                release_class,
                                release_id,
                                self.corpus,
                                annotation=proc_name,
                            )
                            count += 1
                            if count % 1000 == 0:
                                log.info(
                                    f"[{proc_name}] processed {count} of {total_count}"
                                )
                        except DatabaseError as e:
                            log.debug(
                                f"Database record locked in updating relation {max_attempts} goes left",
                                e,
                            )
                            session.rollback()
                            max_attempts -= 1
                            error = True
                            time.sleep(100 - max_attempts)
                        else:
                            session.commit()
                    if error:
                        log.debug(
                            f"Error in updating relations for release: {release_id}"
                        )
                        # except DatabaseError:
                        #     log.exception("Error in LoaderPassOneWorker")
                log.info(f"[{proc_name}] processed {count} of {total_count}")

    word_pattern = re.compile(r"\s+")

    # PRIVATE METHODS

    @classmethod
    def _as_artist_credits(cls, companies):
        artists = []
        for company in companies:
            artist = {
                "name": company["name"],
                "id": company["id"],
                "roles": [{"name": company["entity_type_name"]}],
            }
            artists.append(artist)
        return artists

    # PUBLIC METHODS

    def as_json(self):
        data = {
            "key": self.link_key,
            "role": self.role,
            "source": self.json_entity_one_key,
            "target": self.json_entity_two_key,
        }
        if hasattr(self, "distance"):
            data["distance"] = self.distance
        if hasattr(self, "pages"):
            data["pages"] = tuple(sorted(self.pages))
        return data

    @classmethod
    def loader_pass_one(cls, date: str):
        log.debug("relation loader pass one")
        with DatabaseHelper.session_factory() as session:
            with session.begin():
                relation_class_name = cls.__qualname__
                relation_module_name = cls.__module__
                release_class_name = relation_class_name.replace("Relation", "Release")
                release_module_name = relation_module_name.replace(
                    "relation", "release"
                )
                release_class = getattr(
                    sys.modules[release_module_name], release_class_name
                )

                indices = release_class.get_chunked_release_ids(session)
                workers = [cls.LoaderPassOneWorker(cls, _) for _ in indices]
                log.debug(f"relation loader pass one - start {len(workers)} workers")
                for worker in workers:
                    worker.start()
                for worker in workers:
                    worker.join()
                    if worker.exitcode > 0:
                        log.error(
                            f"relation loader worker {worker.name} exitcode: {worker.exitcode}"
                        )
                        # raise Exception("Error in worker process")
                for worker in workers:
                    worker.terminate()

    # noinspection PyUnusedLocal
    @classmethod
    def loader_pass_one_inner(
        cls, session: Session, release_cls, release_id, corpus, annotation=""
    ):
        try:
            # log.debug(f"loader_pass_one ({annotation}): {release_id}")
            release = session.scalars(
                select(release_cls).where(
                    cast("ColumnElement[bool]", release_cls.release_id == release_id)
                )
            ).one()
        except NoResultFound:
            log.debug(f"            loader_pass_one_inner id not found: {release_id}")
        else:
            relations = cls.from_release(release)
            if LOGGING_TRACE:
                log.debug(
                    f"{cls.__name__.upper()} (Pass 1) [{annotation}]\t"
                    + f"(id:{release.id})\t[{len(relations)}] {release.title}"
                )
            for relation in relations:
                # log.debug(f"  loader_pass_one ({annotation}): {relation}")
                cls.create_relation(session, relation)
                cls.update_relation(session, relation)

    @classmethod
    def create_relation(cls, session: Session, relation):
        try:
            new_instance = cls(
                entity_one_type=relation["entity_one_type"],
                entity_one_id=relation["entity_one_id"],
                entity_two_type=relation["entity_two_type"],
                entity_two_id=relation["entity_two_id"],
                role=relation["role"],
                releases={},
                random=random.random(),
            )
            # log.debug(f"new relation: {new_instance}")
            session.add(new_instance)
            session.commit()
        except IntegrityError:
            session.rollback()

    @classmethod
    def update_relation(cls, session: Session, relation):
        existing_instance = session.scalars(
            select(cls)
            .where(
                (cls.entity_one_type == relation["entity_one_type"])
                & (cls.entity_one_id == relation["entity_one_id"])
                & (cls.entity_two_type == relation["entity_two_type"])
                & (cls.entity_two_id == relation["entity_two_id"])
                & (cls.role == relation["role"])
            )
            .with_for_update()
        ).one()

        if "release_id" in relation:
            release_id: str = str(relation["release_id"])
            # log.debug(f"before update instance: {updated_instance}")
            if release_id not in existing_instance.releases:
                existing_instance.releases[release_id] = relation.get("year")
                # log.debug(f"relation updated releases: {existing_instance}")
                flag_modified(existing_instance, cls.releases.key)
                session.commit()

    @classmethod
    def from_release(cls, release):
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
                    role = Role.normalize(role)
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
                role = Role.normalize(role)
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
                        role = Role.normalize(role)
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
    def from_triples(cls, triples, release=None):
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

    @classmethod
    def search(
        cls,
        entity_id: int,
        entity_type: EntityType = EntityType.ARTIST,
        roles=None,
        # year=None,
        query_only=False,
    ):
        where_clause = (cls.entity_one_id == entity_id) & (
            cls.entity_one_type == entity_type
        )
        where_clause |= (cls.entity_two_id == entity_id) & (
            cls.entity_two_type == entity_type
        )
        if roles:
            where_clause &= cls.role.in_(roles)
        # TODO search by year
        # if year is not None:
        #     year_clause = cls.year.is_null(True)
        #     if isinstance(year, int):
        #         year_clause |= cls.year == year
        #     else:
        #         year_clause |= cls.year.between(year[0], year[1])
        #     where_clause &= year_clause
        return where_clause
        # relation = session.scalar(select(cls).where(where_clause))
        # if query_only:
        #     return relation
        # return [relation]

    @classmethod
    def search_multi(cls, session: Session, entity_keys, roles=None):
        assert entity_keys
        artist_ids, label_ids = [], []
        log.debug(f"entity_keys: {entity_keys}")
        log.debug(f"roles: {roles}")
        for entity_type, entity_id in entity_keys:
            if entity_type == EntityType.ARTIST:
                artist_ids.append(entity_id)
            elif entity_type == EntityType.LABEL:
                label_ids.append(entity_id)
        if artist_ids:
            artist_where_clause = (
                cast("ColumnElement[bool]", cls.entity_one_type == EntityType.ARTIST)
                & cast("ColumnElement[bool]", cls.entity_one_id.in_(artist_ids))
            ) | (
                cast("ColumnElement[bool]", cls.entity_two_type == EntityType.ARTIST)
                & cast("ColumnElement[bool]", cls.entity_two_id.in_(artist_ids))
            )
        else:
            artist_where_clause = None
        if label_ids:
            label_where_clause = (
                cast("ColumnElement[bool]", cls.entity_one_type == EntityType.LABEL)
                & cast("ColumnElement[bool]", cls.entity_one_id.in_(label_ids))
            ) | (
                cast("ColumnElement[bool]", cls.entity_two_type == EntityType.LABEL)
                & cast("ColumnElement[bool]", cls.entity_two_id.in_(label_ids))
            )
        else:
            label_where_clause = None
        if artist_ids and label_ids:
            where_clause = artist_where_clause | label_where_clause
        elif artist_ids:
            where_clause = artist_where_clause
        elif label_ids:
            where_clause = label_where_clause
        else:
            where_clause = None
        if roles:
            if where_clause:
                where_clause = where_clause & (cls.role.in_(roles))
            else:
                where_clause = cls.role.in_(roles)
        log.debug(f"where_clause: {where_clause}")

        relation_results = session.scalars(select(cls).where(where_clause)).all()
        relations = {}
        for relation in relation_results:
            relations[relation.link_key] = relation
        return relations

    # noinspection PyUnusedLocal
    @classmethod
    def search_bimulti(
        cls,
        session: Session,
        lh_entities,
        rh_entities,
        roles=None,
        year=None,
        verbose=True,
    ):
        def build_query(_lh_type, _lh_ids, _rh_type, _rh_ids) -> cls:
            where_clause = cls.entity_one_type == _lh_type
            where_clause &= cls.entity_two_type == _rh_type
            where_clause &= cls.entity_one_id.in_(_lh_ids)
            where_clause &= cls.entity_two_id.in_(_rh_ids)
            if roles:
                where_clause &= cls.role.in_(roles)
            # TODO search by year
            # if year is not None:
            #     year_clause = cls.year.is_null(True)
            #     if isinstance(year, int):
            #         year_clause |= cls.year == year
            #     else:
            #         year_clause |= cls.year.between(year[0], year[1])
            #     where_clause &= year_clause
            relation_result = session.scalars(select(cls).where(where_clause)).one()
            return relation_result

        lh_artist_ids = []
        lh_label_ids = []
        rh_artist_ids = []
        rh_label_ids = []
        for entity_type, entity_id in lh_entities:
            if entity_type == EntityType.ARTIST:
                lh_artist_ids.append(entity_id)
            else:
                lh_label_ids.append(entity_id)
        for entity_type, entity_id in rh_entities:
            if entity_type == EntityType.ARTIST:
                rh_artist_ids.append(entity_id)
            else:
                rh_label_ids.append(entity_id)
        relations = []
        if lh_artist_ids:
            lh_type, lh_ids = 1, lh_artist_ids
            if rh_artist_ids:
                rh_type, rh_ids = 1, rh_artist_ids
                relation = build_query(lh_type, lh_ids, rh_type, rh_ids)
                relations.append(relation)
            if rh_label_ids:
                rh_type, rh_ids = 2, rh_label_ids
                relation = build_query(lh_type, lh_ids, rh_type, rh_ids)
                relations.append(relation)
        if lh_label_ids:
            lh_type, lh_ids = 2, lh_label_ids
            if rh_artist_ids:
                rh_type, rh_ids = 1, rh_artist_ids
                relation = build_query(lh_type, lh_ids, rh_type, rh_ids)
                relations.append(relation)
            if rh_label_ids:
                rh_type, rh_ids = 2, rh_label_ids
                relation = build_query(lh_type, lh_ids, rh_type, rh_ids)
                relations.append(relation)
        # for query in queries:
        #     log.debug(f"search_bimulti query: {query}")
        #     relations.extend(query)
        relation_links = {relation.link_key: relation for relation in relations}
        log.debug(f"relation_links: {relation_links}")
        return relation_links

    # PUBLIC PROPERTIES

    @property
    def entity_one_key(self) -> tuple[int, EntityType]:
        return self.entity_one_id, self.entity_one_type

    @property
    def entity_two_key(self) -> tuple[int, EntityType]:
        return self.entity_two_id, self.entity_two_type

    @property
    def json_entity_one_key(self) -> str:
        if self.entity_one_type == EntityType.ARTIST:
            return f"artist-{self.entity_one_id}"
        elif self.entity_one_type == EntityType.LABEL:
            return f"label-{self.entity_one_id}"
        raise ValueError(self.entity_one_key)

    @property
    def json_entity_two_key(self) -> str:
        if self.entity_two_type == EntityType.ARTIST:
            return f"artist-{self.entity_two_id}"
        elif self.entity_two_type == EntityType.LABEL:
            return f"label-{self.entity_two_id}"
        raise ValueError(self.entity_two_key)

    @property
    def link_key(self) -> str:
        source = self.json_entity_one_key
        target = self.json_entity_two_key
        role = self.word_pattern.sub("-", str(self.role)).lower()
        pieces = [
            source,
            role,
            target,
        ]
        return "-".join(str(_) for _ in pieces)

    @classmethod
    def get_random_relation(cls, session: Session, roles: list[str] = None):
        while True:
            n: float = random.random()
            where_clause = cls.random > n
            if roles:
                where_clause &= cls.role.in_(roles)
            relation = session.scalars(
                select(cls).where(where_clause).order_by(cls.random, cls.role).limit(1)
            ).one_or_none()
            if relation:
                break

        log.debug(f"random relation: {relation}")
        return relation
