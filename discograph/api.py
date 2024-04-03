import logging

from flask import Blueprint
from flask import jsonify
from flask import request

import discograph.utils
from discograph import exceptions, decorators
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
        raise exceptions.APIError(message="Bad Entity Type", status_code=400)
    if not entity_id.isnumeric():
        raise exceptions.APIError(message="Bad Entity Id", status_code=400)
    entity_id = int(entity_id)
    data = DatabaseHelper.db_helper.get_relations(
        DatabaseHelper.flask_db_session(),
        entity_id,
        entity_type,
    )
    if data is None:
        raise exceptions.APIError(message="No Data", status_code=404)
    return jsonify(data)


@blueprint.route("/<entity_type>/network/<entity_id>")
@decorators.limit(max_requests=60, period=60)
def route__api__entity_type__network__entity_id(entity_type, entity_id):
    from discograph.library.database.database_helper import DatabaseHelper

    log.debug(f"entityType: {entity_type}")
    entity_type = EntityType[entity_type.upper()]
    if entity_type not in (EntityType.ARTIST, EntityType.LABEL):
        raise exceptions.APIError(message="Bad Entity Type", status_code=400)
    if not entity_id.isnumeric():
        raise exceptions.APIError(message="Bad Entity Id", status_code=400)
    entity_id = int(entity_id)
    parsed_args = discograph.utils.parse_request_args(request.args)
    original_roles, original_year = parsed_args
    # noinspection PyUnresolvedReferences
    on_mobile = request.MOBILE
    data = DatabaseHelper.db_helper.get_network(
        DatabaseHelper.flask_db_session(),
        entity_id,
        entity_type,
        on_mobile=on_mobile,
        roles=original_roles,
    )
    if data is None:
        raise exceptions.APIError(message="No Data", status_code=404)
    return jsonify(data)


@blueprint.route("/search/<search_string>")
@decorators.limit(max_requests=120, period=60)
def route__api__search(search_string):
    from discograph.library.database.database_helper import DatabaseHelper

    log.debug(f"search_string: {search_string}")
    data = DatabaseHelper.db_helper.search_entities(
        DatabaseHelper.flask_db_session(), search_string
    )
    return jsonify(data)


@blueprint.route("/random")
@decorators.limit(max_requests=60, period=60)
def route__api__random():
    from discograph.library.database.database_helper import DatabaseHelper

    parsed_args = discograph.utils.parse_request_args(request.args)
    original_roles, original_year = parsed_args
    log.debug(f"Roles: {original_roles}")
    try:
        entity_id, entity_type = DatabaseHelper.db_helper.get_random_entity(
            DatabaseHelper.flask_db_session(),
            roles=original_roles,
        )
        log.debug(f"    Found random entity: {entity_type}-{entity_id}")
    except Exception as e:
        log.error(f"{e}")
        raise exceptions.APIError(message="Database error", status_code=503)

    data = {f"center": f"{entity_type.lower()}-{entity_id}"}
    return jsonify(data)
