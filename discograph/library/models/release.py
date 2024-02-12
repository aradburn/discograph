import logging
import multiprocessing
import sys

import peewee

import discograph.config
import discograph.database
import discograph.utils
from discograph import utils
from discograph.library.bootstrapper import Bootstrapper
from discograph.library.discogs_model import DiscogsModel, database_proxy

log = logging.getLogger(__name__)


class Release(DiscogsModel):
    # CLASS VARIABLES

    _artists_mapping = {}

    _companies_mapping = {}

    _tracks_mapping = {}

    class BootstrapPassTwoWorker(multiprocessing.Process):
        def __init__(self, model_class, indices):
            super().__init__()
            self.model_class = model_class
            self.indices = indices

        def run(self):
            proc_name = self.name
            corpus = {}
            total = len(self.indices)
            from discograph.database import bootstrap_database

            if bootstrap_database:
                database_proxy.initialize(bootstrap_database)
            with DiscogsModel.connection_context():
                for i, release_id in enumerate(self.indices):
                    with DiscogsModel.atomic():
                        progress = float(i) / total
                        try:
                            self.model_class.bootstrap_pass_two_single(
                                release_id=release_id,
                                annotation=proc_name,
                                corpus=corpus,
                                progress=progress,
                            )
                        except peewee.PeeweeException:
                            log.exception("ERROR:", release_id, proc_name)

    # PEEWEE FIELDS
    id: peewee.IntegerField
    artists: peewee.Field
    companies: peewee.Field
    country: peewee.TextField
    extra_artists: peewee.Field
    formats: peewee.Field
    genres: peewee.Field
    identifiers: peewee.Field
    labels: peewee.Field
    master_id: peewee.IntegerField
    notes: peewee.TextField
    release_date: peewee.DateTimeField
    styles: peewee.Field
    title: peewee.TextField
    tracklist: peewee.Field

    # PEEWEE META

    class Meta:
        db_table = "releases"

    # PUBLIC METHODS

    @classmethod
    def bootstrap(cls):
        cls.drop_table(True)
        cls.create_table()
        cls.bootstrap_pass_one()
        cls.bootstrap_pass_two()

    @classmethod
    def bootstrap_pass_one(cls, **kwargs):
        log.debug("release bootstrap pass one")
        DiscogsModel.bootstrap_pass_one(
            model_class=cls,
            xml_tag="release",
            name_attr="title",
            skip_without=["title"],
        )

    @classmethod
    def get_indices(cls):
        query = cls.select(cls.id)
        query = query.order_by(cls.id)
        query = query.tuples()
        all_ids = tuple(_[0] for _ in query)
        num_chunks = discograph.database.get_concurrency_count()
        return utils.split_tuple(num_chunks, all_ids)

    @classmethod
    def get_release_iterator(cls):
        id_query = cls.select(cls.id)
        for release in id_query:
            release_id = release.id
            release = cls.select().where(cls.id == release_id).get()
            yield release

    @classmethod
    def bootstrap_pass_two(cls, **kwargs):
        log.debug("release bootstrap pass two")
        indices = cls.get_indices()

        workers = [cls.BootstrapPassTwoWorker(cls, x) for x in indices]
        log.debug(f"release bootstrap pass two - start {len(workers)} workers")
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
            if worker.exitcode > 0:
                log.debug(f"worker.exitcode: {worker.exitcode}")
                # raise Exception("Error in worker process")
        for worker in workers:
            worker.terminate()
        log.debug("release bootstrap pass two - done")

    @classmethod
    def bootstrap_pass_two_single(
        cls,
        release_id,
        annotation="",
        corpus=None,
        progress=None,
    ):
        query = cls.select().where(cls.id == release_id)
        if not query.count():
            return
        document = query.get()
        changed = document.resolve_references(corpus)
        if not changed:
            if Bootstrapper.is_test:
                log.debug(
                    f"{cls.__name__.upper()} (Pass 2) {progress:.3%} [{annotation}]\t"
                    + f"[SKIPPED] (id:{document.id}): {document.title}"
                )
            return
        document.save()
        if Bootstrapper.is_test:
            log.debug(
                f"{cls.__name__.upper()} (Pass 2) {progress:.3%} [{annotation}]\t"
                + f"          (id:{document.id}): {document.title}"
            )

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
                descriptions = Bootstrapper.element_to_strings(sub_element)
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
    def from_element(cls, element):
        data = cls.tags_to_fields(element)
        data["id"] = int(element.get("id"))
        # noinspection PyArgumentList
        return cls(**data)

    def resolve_references(self, corpus, spuriously=False):
        changed = False
        spurious_id = 0
        for entry in self.labels:
            name = entry["name"]
            entity_key = (2, name)
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
    "country": ("country", Bootstrapper.element_to_string),
    "extraartists": ("extra_artists", Release.element_to_artist_credits),
    "formats": ("formats", Release.element_to_formats),
    "genres": ("genres", Bootstrapper.element_to_strings),
    "identifiers": ("identifiers", Release.element_to_identifiers),
    "labels": ("labels", Release.element_to_label_credits),
    "master_id": ("master_id", Bootstrapper.element_to_integer),
    "released": ("release_date", Bootstrapper.element_to_datetime),
    "styles": ("styles", Bootstrapper.element_to_strings),
    "title": ("title", Bootstrapper.element_to_string),
    "tracklist": ("tracklist", Release.element_to_tracks),
}

Release._artists_mapping = {
    "id": ("id", Bootstrapper.element_to_integer),
    "name": ("name", Bootstrapper.element_to_string),
    "anv": ("anv", Bootstrapper.element_to_string),
    "join": ("join", Bootstrapper.element_to_string),
    "role": ("roles", Release.element_to_roles),
    "tracks": ("tracks", Bootstrapper.element_to_string),
}

Release._companies_mapping = {
    "id": ("id", Bootstrapper.element_to_integer),
    "name": ("name", Bootstrapper.element_to_string),
    "catno": ("catalog_number", Bootstrapper.element_to_string),
    "entity_type": ("entity_type", Bootstrapper.element_to_integer),
    "entity_type_name": ("entity_type_name", Bootstrapper.element_to_string),
}

Release._tracks_mapping = {
    "position": ("position", Bootstrapper.element_to_string),
    "title": ("title", Bootstrapper.element_to_string),
    "duration": ("duration", Bootstrapper.element_to_string),
    "artists": ("artists", Release.element_to_artist_credits),
    "extraartists": ("extra_artists", Release.element_to_artist_credits),
}
