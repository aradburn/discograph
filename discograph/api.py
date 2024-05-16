import logging

from flask import Blueprint
from flask import jsonify
from flask import request

import discograph.utils
from discograph import decorators
from discograph.exceptions import BadRequestError, NotFoundError, DatabaseError
from discograph.library.database.transaction import transaction
from discograph.library.fields.entity_type import EntityType

log = logging.getLogger(__name__)

blueprint = Blueprint("api", __name__, template_folder="templates")


@blueprint.route("/<entity_type>/relations/<entity_id>")
@decorators.limit(max_requests=60, period=60)
def route__api__entity_type__relations__entity_id(entity_type, entity_id):
    from discograph.library.database.database_helper import DatabaseHelper

    log.debug(f"entityType: {entity_type}")
    entity_type = EntityType[entity_type.upper()]
    if entity_type not in (EntityType.ARTIST, EntityType.LABEL):
        raise BadRequestError(message="Bad Entity Type")
    if not entity_id.isnumeric():
        raise BadRequestError(message="Bad Entity Id")
    entity_id = int(entity_id)
    with transaction():
        data = DatabaseHelper.db_helper.get_relations(
            entity_id,
            entity_type,
        )
    if data is None:
        raise NotFoundError(message="No Data")
    return jsonify(data)


@blueprint.route("/<entity_type>/network/<entity_id>")
@decorators.limit(max_requests=60, period=60)
def route__api__entity_type__network__entity_id(entity_type, entity_id):
    from discograph.library.database.database_helper import DatabaseHelper

    log.debug(f"entityType: {entity_type}")
    entity_type = EntityType[entity_type.upper()]
    if entity_type not in (EntityType.ARTIST, EntityType.LABEL):
        raise BadRequestError(message="Bad Entity Type")
    if not entity_id.isnumeric():
        raise BadRequestError(message="Bad Entity Id")
    entity_id = int(entity_id)
    parsed_args = discograph.utils.parse_request_args(request.args)
    original_roles, original_year = parsed_args
    # noinspection PyUnresolvedReferences
    on_mobile = request.MOBILE
    with transaction():
        data = DatabaseHelper.db_helper.get_network(
            entity_id,
            entity_type,
            on_mobile=on_mobile,
            roles=original_roles,
        )
    if data is None:
        raise NotFoundError(message="No Data")
    return jsonify(data)


@blueprint.route("/search/<search_string>")
@decorators.limit(max_requests=120, period=60)
def route__api__search(search_string):
    from discograph.library.database.database_helper import DatabaseHelper

    log.debug(f"search_string: {search_string}")
    with transaction():
        data = DatabaseHelper.db_helper.search_entities(search_string)
    return jsonify(data)


@blueprint.route("/random")
@decorators.limit(max_requests=60, period=60)
def route__api__random():
    from discograph.library.database.database_helper import DatabaseHelper

    parsed_args = discograph.utils.parse_request_args(request.args)
    original_roles, original_year = parsed_args
    log.debug(f"Roles: {original_roles}")
    with transaction():
        try:
            entity_id, entity_type = DatabaseHelper.db_helper.get_random_entity(
                roles=original_roles,
            )
            log.debug(f"    Found random entity: {entity_type}-{entity_id}")
        except Exception as e:
            log.error(f"{e}")
            raise DatabaseError(message="Database error")

    data = {f"center": f"{entity_type.name.lower()}-{entity_id}"}
    return jsonify(data)
