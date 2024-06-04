import logging

log = logging.getLogger(__name__)


class ReleaseDataAccess:
    pass

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
