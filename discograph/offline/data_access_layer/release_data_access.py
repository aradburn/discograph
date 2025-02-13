import logging

from discograph.offline.database.release_repository import ReleaseRepository
from discograph.library.full_text_search.entity_details_index import EntityDetailsIndex
from discograph.offline.loader.loader_base import LoaderBase

log = logging.getLogger(__name__)


class ReleaseDataAccess:
    @staticmethod
    def init_entity_details_index(
        release_repository: ReleaseRepository, index: EntityDetailsIndex
    ) -> None:
        count = 0
        for release in release_repository.all():
            country = release.country
            if country is not None:
                for artist in release.artists:
                    if "id" in artist:
                        index.index_country(artist["id"], country)
                for label in release.labels:
                    if "id" in label:
                        index.index_country(label["id"], country)

            count += 1
            if count % (LoaderBase.BULK_REPORTING_SIZE * 100) == 0:
                log.debug(f"Indexed {count} releases")
        index.print_sizes()

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
