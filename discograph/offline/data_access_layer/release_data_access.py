import logging

from discograph.offline.database.release_repository import ReleaseRepository
from discograph.library.full_text_search.entity_details_index import EntityDetailsIndex
from discograph.offline.loader.loader_base import LoaderBase

log = logging.getLogger(__name__)


class ReleaseDataAccess:
    @staticmethod
    def create_entity_details_index(
        release_repository: ReleaseRepository,
    ) -> EntityDetailsIndex:
        log.debug("Create EntityDetailsIndex")
        entity_details_index = EntityDetailsIndex()
        count = 0
        for release in release_repository.all():
            country = release.country
            if country is not None:
                for artist in release.artists:
                    if "id" in artist:
                        entity_details_index.index_country(artist["id"], country)
                for label in release.labels:
                    if "id" in label:
                        entity_details_index.index_country(label["id"], country)

            if release.genres is not None:
                for genre in release.genres:
                    for artist in release.artists:
                        if "id" in artist:
                            entity_details_index.index_genre(artist["id"], genre)
                    for label in release.labels:
                        if "id" in label:
                            entity_details_index.index_genre(label["id"], genre)

            if release.styles is not None:
                for style in release.styles:
                    for artist in release.artists:
                        if "id" in artist:
                            entity_details_index.index_style(artist["id"], style)
                    for label in release.labels:
                        if "id" in label:
                            entity_details_index.index_style(label["id"], style)

            count += 1
            if count % (LoaderBase.BULK_REPORTING_SIZE * 100) == 0:
                log.debug(f"Indexed {count} releases")
        if count % (LoaderBase.BULK_REPORTING_SIZE * 100) != 0:
            log.debug(f"Indexed {count} releases")
        entity_details_index.print_details()
        entity_details_index.print_sizes()
        return entity_details_index

    # @classmethod
    # def _as_artist_credits(cls, companies):
    #     artists = []
    #     for company in companies:
    #         artist = {
    #             "name": company["name"],
    #             "id": company["id"],
    #             "roles": [{"name": company["entity_type_name"]}],
    #         }
    #         artists.append(artist)
    #     return artists
