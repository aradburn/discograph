import logging
from random import random

from discograph.library.database.release_repository import ReleaseRepository
from discograph.library.database.release_table import ReleaseTable
from discograph.library.database.transaction import transaction
from discograph.library.domain.release import Release
from discograph.library.loader.loader_base import LoaderBase
from discograph.library.loader.worker_release_pass_two import WorkerReleasePassTwo
from discograph.library.loader_utils import LoaderUtils
from discograph.utils import timeit

log = logging.getLogger(__name__)


class LoaderRelease(LoaderBase):
    # CLASS VARIABLES

    _artists_mapping = {}

    _companies_mapping = {}

    _tracks_mapping = {}

    @classmethod
    @timeit
    def loader_pass_one(cls, date: str) -> int:
        log.debug("release loader pass one")
        releases_loaded = cls.loader_pass_one_manager(
            repository=ReleaseRepository(),
            date=date,
            xml_tag="release",
            id_attr=ReleaseTable.release_id.name,
            name_attr="title",
            skip_without=["title"],
        )
        return releases_loaded

    @classmethod
    @timeit
    def loader_pass_two(cls):
        log.debug("release loader pass two")
        with transaction():
            repository = ReleaseRepository()
            chunked_release_ids = repository.get_chunked_release_ids()

            workers = [
                WorkerReleasePassTwo(release_ids) for release_ids in chunked_release_ids
            ]
            log.debug(f"release loader pass two - start {len(workers)} workers")
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join()
                if worker.exitcode > 0:
                    log.debug(
                        f"release loader worker {worker.name} exitcode: {worker.exitcode}"
                    )
                    raise Exception("Error in worker process")
            for worker in workers:
                worker.terminate()
            log.debug("release loader pass two - done")

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
        # log.debug(f"credit_roles: {credit_roles}")
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

    # @classmethod
    # def element_to_genres(cls, session: Session, element):
    #     result = []
    #     if element is None or not len(element):
    #         return result
    #     for sub_element in element:
    #         genre_str = sub_element.text
    #         genre = Genre.create_or_get(session, genre_str)
    #         result.append(genre)
    #     return result

    @classmethod
    def from_element(cls, element) -> Release:
        data = cls.tags_to_fields(element)
        data["release_id"] = int(element.get("id"))
        if "identifiers" not in data:
            data["identifiers"] = None
        if "master_id" not in data:
            data["master_id"] = None
        if "notes" not in data:
            data["notes"] = None
        data["random"] = random()
        return Release(**data)


LoaderRelease._tags_to_fields_mapping = {
    "artists": ("artists", LoaderRelease.element_to_artist_credits),
    "companies": ("companies", LoaderRelease.element_to_company_credits),
    "country": ("country", LoaderUtils.element_to_string),
    "extraartists": ("extra_artists", LoaderRelease.element_to_artist_credits),
    "formats": ("formats", LoaderRelease.element_to_formats),
    # "genres": ("genres", Release.element_to_genres),
    "genres": ("genres", LoaderUtils.element_to_strings),
    "identifiers": ("identifiers", LoaderRelease.element_to_identifiers),
    "labels": ("labels", LoaderRelease.element_to_label_credits),
    "master_id": ("master_id", LoaderUtils.element_to_integer),
    "notes": ("notes", LoaderUtils.element_to_none),
    "released": ("release_date", LoaderUtils.element_to_datetime),
    "styles": ("styles", LoaderUtils.element_to_strings),
    "title": ("title", LoaderUtils.element_to_string),
    "tracklist": ("tracklist", LoaderRelease.element_to_tracks),
}

LoaderRelease._artists_mapping = {
    "id": ("id", LoaderUtils.element_to_integer),
    "name": ("name", LoaderUtils.element_to_string),
    "anv": ("anv", LoaderUtils.element_to_string),
    "join": ("join", LoaderUtils.element_to_string),
    "role": ("roles", LoaderRelease.element_to_roles),
    "tracks": ("tracks", LoaderUtils.element_to_string),
}

LoaderRelease._companies_mapping = {
    "id": ("id", LoaderUtils.element_to_integer),
    "name": ("name", LoaderUtils.element_to_string),
    "catno": ("catalog_number", LoaderUtils.element_to_string),
    "entity_type": ("entity_type", LoaderUtils.element_to_integer),
    "entity_type_name": ("entity_type_name", LoaderUtils.element_to_string),
}

LoaderRelease._tracks_mapping = {
    "position": ("position", LoaderUtils.element_to_string),
    "title": ("title", LoaderUtils.element_to_string),
    "duration": ("duration", LoaderUtils.element_to_string),
    "artists": ("artists", LoaderRelease.element_to_artist_credits),
    "extraartists": ("extra_artists", LoaderRelease.element_to_artist_credits),
}
