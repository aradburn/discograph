import logging

log = logging.getLogger(__name__)


# class UpdaterEntity(UpdaterBase):
#     @classmethod
#     @timeit
#     def updater_pass_one(cls, date: str):
#         log.debug("entity updater pass one - artist")
#         with transaction():
#             entity_repository = EntityRepository()
#             cls.updater_pass_one_manager(
#                 repository=entity_repository,
#                 date=date,
#                 xml_tag="artist",
#                 id_attr=EntityTable.entity_id.name,
#                 skip_without=["entity_name"],
#             )
#         log.debug("entity updater pass one - label")
#         with transaction():
#             entity_repository = EntityRepository()
#             cls.updater_pass_one_manager(
#                 repository=entity_repository,
#                 date=date,
#                 xml_tag="label",
#                 id_attr=EntityTable.entity_id.name,
#                 skip_without=["entity_name"],
#             )
#
#     @classmethod
#     def update_bulk(cls, bulk_updates, processed_count):
#         worker = WorkerEntityUpdater(bulk_updates, processed_count)
#         return worker
#
#     @classmethod
#     def preprocess_data(cls, data, element):
#         data["entity_metadata"] = {}
#         data["entities"] = {}
#         data["relation_counts"] = {}
#         for key in (
#             "aliases",
#             "groups",
#             "members",
#             "parent_label",
#             "sublabels",
#         ):
#             if key in data:
#                 data["entities"][key] = data.pop(key)
#         for key in (
#             "contact_info",
#             "name_variations",
#             "profile",
#             "real_name",
#             "urls",
#         ):
#             if key in data:
#                 data["entity_metadata"][key] = data.pop(key)
#         if "entity_name" in data and data.get("entity_name"):
#             name = data.get("entity_name")
#             data["search_content"] = normalise_search_content(name)
#         if element.tag == "artist":
#             data["entity_type"] = EntityType.ARTIST
#         elif element.tag == "label":
#             data["entity_type"] = EntityType.LABEL
#         return data
#
#
# UpdaterEntity._tags_to_fields_mapping = {
#     "aliases": ("aliases", LoaderEntity.element_to_names),
#     "contact_info": ("contact_info", LoaderUtils.element_to_string),
#     "groups": ("groups", LoaderEntity.element_to_names),
#     "id": ("entity_id", LoaderUtils.element_to_integer),
#     "members": ("members", LoaderEntity.element_to_names_and_ids),
#     "name": ("entity_name", LoaderUtils.element_to_string),
#     "namevariations": ("name_variations", LoaderUtils.element_to_strings),
#     "parentLabel": ("parent_label", LoaderEntity.element_to_parent_label),
#     "profile": ("profile", LoaderUtils.element_to_string),
#     "realname": ("real_name", LoaderUtils.element_to_string),
#     "sublabels": ("sublabels", LoaderEntity.element_to_sublabels),
#     "urls": ("urls", LoaderUtils.element_to_strings),
# }
