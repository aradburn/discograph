import gzip
import logging
import multiprocessing
import pprint
import random
import sys
from typing import List

import peewee
from deepdiff import DeepDiff

from discograph import utils
from discograph.database import get_concurrency_count
from discograph.library.database.database_worker import DatabaseWorker
from discograph.library.discogs_model import DiscogsModel
from discograph.library.fields.entity_type import EntityType
from discograph.library.loader_utils import LoaderUtils
from discograph.library.models.genre import Genre
from discograph.logging_config import LOGGING_TRACE

log = logging.getLogger(__name__)


class Release(DiscogsModel):
    # CLASS VARIABLES

    _artists_mapping = {}

    _companies_mapping = {}

    _tracks_mapping = {}

    class LoaderPassTwoWorker(multiprocessing.Process):
        def __init__(self, model_class, indices):
            super().__init__()
            self.model_class = model_class
            self.indices = indices

        def run(self):
            proc_name = self.name
            corpus = {}
            total = len(self.indices)
            from discograph.library.database.database_proxy import database_proxy

            database_proxy.initialize(DatabaseWorker.worker_database)

            with DiscogsModel.connection_context():
                for i, release_id in enumerate(self.indices):
                    with DiscogsModel.atomic():
                        progress = float(i) / total
                        try:
                            self.model_class.loader_pass_two_single(
                                release_id=release_id,
                                annotation=proc_name,
                                corpus=corpus,
                                progress=progress,
                            )
                        except peewee.PeeweeException:
                            log.exception("ERROR:", release_id, proc_name)

    class UpdaterPassOneWorker(multiprocessing.Process):
        def __init__(self, model_class, bulk_updates, processed_count):
            super().__init__()
            self.model_class = model_class
            self.bulk_updates = bulk_updates
            self.processed_count = processed_count

        def run(self):
            proc_name = self.name
            from discograph.library.database.database_proxy import database_proxy

            database_proxy.initialize(DatabaseWorker.worker_database)

            updated_count = 0
            with DiscogsModel.connection_context():
                for i, updated_release in enumerate(self.bulk_updates):
                    with DiscogsModel.atomic():
                        try:
                            old_release = self.model_class.get_by_id(
                                updated_release.release_id
                            )

                            differences = DeepDiff(
                                old_release,
                                updated_release,
                                exclude_paths=[
                                    "id",
                                    "random",
                                    "dirty_fields",
                                    "_dirty",
                                    "root.labels[0]['id']",
                                    "root.labels[1]['id']",
                                    "root.labels[2]['id']",
                                    "root.labels[3]['id']",
                                    "root.labels[4]['id']",
                                    "root.labels[5]['id']",
                                    "root.labels[6]['id']",
                                    "root.labels[7]['id']",
                                    "root.labels[8]['id']",
                                    "root.labels[9]['id']",
                                    "root.labels[10]['id']",
                                    "root.labels[11]['id']",
                                    "root.labels[12]['id']",
                                    "root.labels[13]['id']",
                                    "root.labels[14]['id']",
                                    "root.labels[15]['id']",
                                    "root.labels[16]['id']",
                                    "root.labels[17]['id']",
                                    "root.labels[18]['id']",
                                    "root.labels[19]['id']",
                                    "root.labels[20]['id']",
                                ],
                                ignore_numeric_type_changes=True,
                            )
                            diff = pprint.pformat(differences)
                            if diff != "{}":
                                log.debug(f"diff: {diff}")
                                # log.debug(f"old_release: {old_release}")
                                # log.debug(f"updated_release: {updated_release}")

                                # differences2 = DeepDiff(
                                #     old_entity.entities,
                                #     updated_entity.entities,
                                #     ignore_numeric_type_changes=True,
                                # )
                                # diff2 = pprint.pformat(differences2)
                                # if diff2 != "{}":
                                #     log.debug(f"entities diff: {diff2}")
                                # Update release
                                q = self.model_class.update(
                                    {
                                        self.model_class.release_id: updated_release.release_id,
                                        self.model_class.artists: updated_release.artists,
                                        self.model_class.companies: updated_release.companies,
                                        self.model_class.country: updated_release.country,
                                        self.model_class.extra_artists: updated_release.extra_artists,
                                        self.model_class.formats: updated_release.formats,
                                        self.model_class.genres: updated_release.genres,
                                        self.model_class.identifiers: updated_release.identifiers,
                                        self.model_class.labels: updated_release.labels,
                                        self.model_class.master_id: updated_release.master_id,
                                        self.model_class.notes: updated_release.notes,
                                        self.model_class.release_date: updated_release.release_date,
                                        self.model_class.styles: updated_release.styles,
                                        self.model_class.title: updated_release.title,
                                        self.model_class.tracklist: updated_release.tracklist,
                                    }
                                ).where(
                                    self.model_class.release_id
                                    == updated_release.release_id
                                )
                                q.execute()  # Execute the query.
                                updated_count += 1
                        except peewee.PeeweeException as e:
                            log.exception("Error in updater_pass_one")
                            raise e

            log.info(
                f"[{proc_name}] processed_count: {self.processed_count}, updated: {updated_count}"
            )

    # PEEWEE FIELDS
    release_id = peewee.IntegerField(primary_key=True)
    artists: peewee.Field
    companies: peewee.Field
    country: peewee.TextField
    extra_artists: peewee.Field
    formats: peewee.Field
    # genres: peewee.Field
    identifiers: peewee.Field
    labels: peewee.Field
    master_id: peewee.IntegerField
    notes: peewee.TextField
    release_date: peewee.DateTimeField
    styles: peewee.Field
    title: peewee.TextField
    tracklist: peewee.Field
    random: peewee.FloatField

    # PEEWEE META

    class Meta:
        table_name = "release"

    # PUBLIC METHODS
    @classmethod
    def read(cls, release_id):
        from discograph.library.models.release_genre import ReleaseGenre

        query = (
            cls.select(cls, Genre.genre_name.alias("genres"))
            .join(ReleaseGenre, on=(cls.release_id == ReleaseGenre.release_id))
            .join(Genre, on=(ReleaseGenre.genre_id == Genre.genre_id))
            .where(cls.release_id == release_id)
        )
        log.debug(f"query: {query}")
        for result in query.dicts():
            log.debug(f"result: {result}")
        return query.dicts().get()

    @classmethod
    def loader_pass_one(cls, date: str):
        log.debug("release loader pass one")
        DiscogsModel.loader_pass_one_manager(
            model_class=cls,
            date=date,
            xml_tag="release",
            id_attr=cls.release_id.name,
            name_attr="title",
            skip_without=["title"],
        )

    @classmethod
    def updater_pass_one(cls, date: str):
        log.debug("release updater pass one")
        Release.updater_pass_one_manager(
            model_class=cls,
            date=date,
            xml_tag="release",
            id_attr=cls.release_id.name,
            name_attr="title",
            skip_without=["title"],
        )

    @classmethod
    def updater_pass_one_manager(
        cls,
        model_class,
        date: str,
        xml_tag: str,
        id_attr: str,
        name_attr: str,
        skip_without: List[str],
    ):
        # Updater pass one.
        # initial_count = len(model_class)
        processed_count = 0
        xml_path = LoaderUtils.get_xml_path(xml_tag, date)
        log.info(f"Loading data from {xml_path}")
        with gzip.GzipFile(xml_path, "r") as file_pointer:
            iterator = LoaderUtils.iterparse(file_pointer, xml_tag)
            bulk_updates = []
            workers = []
            for i, element in enumerate(iterator):
                data = None
                try:
                    data = model_class.tags_to_fields(element)
                    if skip_without:
                        if any(not data.get(_) for _ in skip_without):
                            continue
                    if element.get("id"):
                        data[id_attr] = int(element.get("id"))
                    updated_release = model_class(model_class, **data)
                    bulk_updates.append(updated_release)
                    processed_count += 1
                    # log.debug(f"updated_release: {updated_release}")
                    if get_concurrency_count() > 1:
                        # Can do multi threading
                        if len(bulk_updates) >= DiscogsModel.BULK_UPDATE_BATCH_SIZE:
                            worker = cls.update_bulk(
                                model_class, bulk_updates, processed_count
                            )
                            worker.start()
                            workers.append(worker)
                            bulk_updates.clear()
                        if len(workers) > get_concurrency_count():
                            worker = workers.pop(0)
                            # log.debug(f"wait for worker {len(workers)} in list")
                            worker.join()
                            if worker.exitcode > 0:
                                log.error(
                                    f"worker {worker.name} exitcode: {worker.exitcode}"
                                )
                                # raise Exception("Error in worker process")
                            worker.terminate()
                    # if inserted_count >= 10:
                    #     break
                    # document = model_class.create(**data)
                    # template = "{} (Pass 1) (idx:{}) (id:{}): {}"
                    # message = template.format(
                    #     model_class.__name__.upper(),
                    #     i,
                    #     getattr(document, id_attr),
                    #     getattr(document, name_attr),
                    # )
                    # log.debug(message)
                except peewee.DataError as e:
                    log.exception("Error in updater_pass_one", pprint.pformat(data))
                    # traceback.print_exc()
                    raise e

            if len(bulk_updates) > 0:
                worker = cls.update_bulk(model_class, bulk_updates, processed_count)
                worker.start()
                workers.append(worker)
                bulk_updates.clear()

            while len(workers) > 0:
                worker = workers.pop(0)
                # log.debug(
                #     f"wait for worker {worker.name} - {len(workers)} left in list"
                # )
                worker.join()
                if worker.exitcode > 0:
                    log.error(f"worker {worker.name} exitcode: {worker.exitcode}")
                    # raise Exception("Error in worker process")
                worker.terminate()

    @classmethod
    def update_bulk(cls, model_class, bulk_updates, processed_count):
        worker = cls.UpdaterPassOneWorker(model_class, bulk_updates, processed_count)
        return worker

    @classmethod
    def get_indices(cls, multiplier=1):
        from discograph.database import get_concurrency_count

        query = cls.select(cls.release_id)
        query = query.order_by(cls.release_id)
        query = query.tuples()
        all_ids = tuple(_[0] for _ in query)
        num_chunks = get_concurrency_count() * multiplier
        return utils.split_tuple(num_chunks, all_ids)

    @classmethod
    def get_release_iterator(cls):
        id_query = cls.select(cls.release_id)
        for release in id_query:
            release_id = release.release_id
            release = cls.select().where(cls.release_id == release_id).get()
            yield release

    @classmethod
    def loader_pass_two(cls, **kwargs):
        log.debug("release loader pass two")
        indices = cls.get_indices()

        workers = [cls.LoaderPassTwoWorker(cls, x) for x in indices]
        log.debug(f"release loader pass two - start {len(workers)} workers")
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
            if worker.exitcode > 0:
                log.debug(
                    f"release loader worker {worker.name} exitcode: {worker.exitcode}"
                )
                # raise Exception("Error in worker process")
        for worker in workers:
            worker.terminate()
        log.debug("release loader pass two - done")

    @classmethod
    def loader_pass_two_single(
        cls,
        release_id,
        annotation="",
        corpus=None,
        progress=None,
    ):
        release = cls.get_by_id(release_id)
        corpus = corpus or {}
        changed = release.resolve_references(corpus)
        if changed:
            if LOGGING_TRACE:
                log.debug(
                    f"{cls.__name__.upper()} (Pass 2) {progress:.3%} [{annotation}]\t"
                    + f"          (id:{release.release_id}): {release.title}"
                )
            release.save()
        elif LOGGING_TRACE:
            log.debug(
                f"{cls.__name__.upper()} (Pass 2) {progress:.3%} [{annotation}]\t"
                + f"[SKIPPED] (id:{release.release_id}): {release.title}"
            )

    @classmethod
    def get_random(cls):
        n = random.random()
        return cls.select().where(cls.random > n).order_by(cls.random).get()

    @classmethod
    def element_to_artist_credits(cls, element):
        result = []
        if element is None or not len(element):
            return result
        for subelement in element:
            data = cls.tags_to_fields(
                subelement,
                ignore_none=True,
                mapping=cls._artists_mapping,
            )
            result.append(data)
        return result

    @classmethod
    def element_to_company_credits(cls, element):
        result = []
        if element is None or not len(element):
            return result
        for subelement in element:
            data = cls.tags_to_fields(
                subelement,
                ignore_none=True,
                mapping=cls._companies_mapping,
            )
            result.append(data)
        return result

    @classmethod
    def element_to_formats(cls, element):
        result = []
        if element is None or not len(element):
            return result
        for sub_element in element:
            document = {
                "name": sub_element.get("name"),
                "quantity": sub_element.get("qty"),
            }
            if sub_element.get("text"):
                document["text"] = sub_element.get("text")
            if len(sub_element):
                sub_element = sub_element[0]
                descriptions = LoaderUtils.element_to_strings(sub_element)
                document["descriptions"] = descriptions
            result.append(document)
        return result

    @classmethod
    def element_to_identifiers(cls, element):
        result = []
        if element is None or not len(element):
            return result
        for sub_element in element:
            data = {
                "description": sub_element.get("description"),
                "type": sub_element.get("type"),
                "value": sub_element.get("value"),
            }
            result.append(data)
        return result

    @classmethod
    def element_to_label_credits(cls, element):
        result = []
        if element is None or not len(element):
            return result
        for sub_element in element:
            data = {
                "catalog_number": sub_element.get("catno"),
                "name": sub_element.get("name"),
            }
            result.append(data)
        return result

    @classmethod
    def element_to_roles(cls, element):
        def from_text(text):
            name = ""
            current_buffer = ""
            details = []
            had_detail = False
            _bracket_depth = 0
            for _character in text:
                if _character == "[":
                    _bracket_depth += 1
                    if _bracket_depth == 1 and not had_detail:
                        name = current_buffer
                        current_buffer = ""
                        had_detail = True
                    elif 1 < _bracket_depth:
                        current_buffer += _character
                elif _character == "]":
                    _bracket_depth -= 1
                    if not _bracket_depth:
                        details.append(current_buffer)
                        current_buffer = ""
                    else:
                        current_buffer += _character
                else:
                    current_buffer += _character
            if current_buffer and not had_detail:
                name = current_buffer
            name = name.strip()
            detail = ", ".join(_.strip() for _ in details)
            result = {"name": name}
            if detail:
                result["detail"] = detail
            return result

        credit_roles = []
        if element is None or not element.text:
            return credit_roles or None
        current_text = ""
        bracket_depth = 0
        for character in element.text:
            if character == "[":
                bracket_depth += 1
            elif character == "]":
                bracket_depth -= 1
            elif not bracket_depth and character == ",":
                current_text = current_text.strip()
                if current_text:
                    credit_roles.append(from_text(current_text))
                current_text = ""
                continue
            current_text += character
        current_text = current_text.strip()
        if current_text:
            credit_roles.append(from_text(current_text))
        log.debug(f"credit_roles: {credit_roles}")
        return credit_roles or None

    @classmethod
    def element_to_tracks(cls, element):
        result = []
        if element is None or not len(element):
            return result
        for sub_element in element:
            data = cls.tags_to_fields(
                sub_element,
                ignore_none=True,
                mapping=cls._tracks_mapping,
            )
            result.append(data)
        return result

    @classmethod
    def element_to_genres(cls, element):
        result = []
        if element is None or not len(element):
            return result
        for sub_element in element:
            genre_str = sub_element.text
            genre = Genre.create_or_get(genre_str)
            result.append(genre)
        return result

    @classmethod
    def from_element(cls, element):
        data = cls.tags_to_fields(element)
        data[cls.release_id.name] = int(element.get("id"))
        # noinspection PyArgumentList
        return cls(**data)

    def resolve_references(self, corpus, spuriously=False):
        changed = False
        spurious_id = 0
        for entry in self.labels:
            name = entry["name"]
            entity_key = (EntityType.LABEL, name)
            if not spuriously:
                release_class_name = self.__class__.__qualname__
                release_module_name = self.__class__.__module__
                entity_class_name = release_class_name.replace("Release", "Entity")
                entity_module_name = release_module_name.replace("release", "entity")
                entity_class = getattr(
                    sys.modules[entity_module_name], entity_class_name
                )

                entity_class.update_corpus(corpus, entity_key)
            if entity_key in corpus:
                entry["id"] = corpus[entity_key]
                changed = True
            elif spuriously:
                spurious_id -= 1
                corpus[entity_key] = spurious_id
                entry["id"] = corpus[entity_key]
                changed = True
        return changed


Release._tags_to_fields_mapping = {
    "artists": ("artists", Release.element_to_artist_credits),
    "companies": ("companies", Release.element_to_company_credits),
    "country": ("country", LoaderUtils.element_to_string),
    "extraartists": ("extra_artists", Release.element_to_artist_credits),
    "formats": ("formats", Release.element_to_formats),
    "genres": ("genres", Release.element_to_genres),
    # "genres": ("genres", LoaderUtils.element_to_strings),
    "identifiers": ("identifiers", Release.element_to_identifiers),
    "labels": ("labels", Release.element_to_label_credits),
    "master_id": ("master_id", LoaderUtils.element_to_integer),
    "released": ("release_date", LoaderUtils.element_to_datetime),
    "styles": ("styles", LoaderUtils.element_to_strings),
    "title": ("title", LoaderUtils.element_to_string),
    "tracklist": ("tracklist", Release.element_to_tracks),
}

Release._artists_mapping = {
    "id": ("id", LoaderUtils.element_to_integer),
    "name": ("name", LoaderUtils.element_to_string),
    "anv": ("anv", LoaderUtils.element_to_string),
    "join": ("join", LoaderUtils.element_to_string),
    "role": ("roles", Release.element_to_roles),
    "tracks": ("tracks", LoaderUtils.element_to_string),
}

Release._companies_mapping = {
    "id": ("id", LoaderUtils.element_to_integer),
    "name": ("name", LoaderUtils.element_to_string),
    "catno": ("catalog_number", LoaderUtils.element_to_string),
    "entity_type": ("entity_type", LoaderUtils.element_to_integer),
    "entity_type_name": ("entity_type_name", LoaderUtils.element_to_string),
}

Release._tracks_mapping = {
    "position": ("position", LoaderUtils.element_to_string),
    "title": ("title", LoaderUtils.element_to_string),
    "duration": ("duration", LoaderUtils.element_to_string),
    "artists": ("artists", Release.element_to_artist_credits),
    "extraartists": ("extra_artists", Release.element_to_artist_credits),
}
