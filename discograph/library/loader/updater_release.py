import logging

log = logging.getLogger(__name__)


# class UpdaterRelease(UpdaterBase):
#     @classmethod
#     @timeit
#     def updater_pass_one(cls, date: str):
#         log.debug("release updater pass one")
#         with transaction():
#             release_repository = ReleaseRepository()
#             cls.updater_pass_one_manager(
#                 repository=release_repository,
#                 date=date,
#                 xml_tag="release",
#                 id_attr=ReleaseTable.release_id.name,
#                 skip_without=["title"],
#             )
#
#     @classmethod
#     def update_bulk(cls, bulk_updates, processed_count):
#         worker = WorkerReleaseUpdater(bulk_updates, processed_count)
#         return worker
#
#
# UpdaterRelease._tags_to_fields_mapping = {
#     "artists": ("artists", LoaderRelease.element_to_artist_credits),
#     "companies": ("companies", LoaderRelease.element_to_company_credits),
#     "country": ("country", LoaderUtils.element_to_string),
#     "extraartists": ("extra_artists", LoaderRelease.element_to_artist_credits),
#     "formats": ("formats", LoaderRelease.element_to_formats),
#     "genres": ("genres", LoaderUtils.element_to_strings),
#     "identifiers": ("identifiers", LoaderRelease.element_to_identifiers),
#     "labels": ("labels", LoaderRelease.element_to_label_credits),
#     "master_id": ("master_id", LoaderUtils.element_to_integer),
#     "notes": ("notes", LoaderUtils.element_to_none),
#     "released": ("release_date", LoaderUtils.element_to_datetime),
#     "styles": ("styles", LoaderUtils.element_to_strings),
#     "title": ("title", LoaderUtils.element_to_string),
#     "tracklist": ("tracklist", LoaderRelease.element_to_tracks),
# }
#
# UpdaterRelease._artists_mapping = {
#     "id": ("id", LoaderUtils.element_to_integer),
#     "name": ("name", LoaderUtils.element_to_string),
#     "anv": ("anv", LoaderUtils.element_to_string),
#     "join": ("join", LoaderUtils.element_to_string),
#     "role": ("roles", LoaderRelease.element_to_roles),
#     "tracks": ("tracks", LoaderUtils.element_to_string),
# }
#
# UpdaterRelease._companies_mapping = {
#     "id": ("id", LoaderUtils.element_to_integer),
#     "name": ("name", LoaderUtils.element_to_string),
#     "catno": ("catalog_number", LoaderUtils.element_to_string),
#     "entity_type": ("entity_type", LoaderUtils.element_to_integer),
#     "entity_type_name": ("entity_type_name", LoaderUtils.element_to_string),
# }
#
# UpdaterRelease._tracks_mapping = {
#     "position": ("position", LoaderUtils.element_to_string),
#     "title": ("title", LoaderUtils.element_to_string),
#     "duration": ("duration", LoaderUtils.element_to_string),
#     "artists": ("artists", LoaderRelease.element_to_artist_credits),
#     "extraartists": ("extra_artists", LoaderRelease.element_to_artist_credits),
# }
