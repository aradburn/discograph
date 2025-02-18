import logging

from flask import Blueprint
from flask import jsonify
from flask import request

import discograph.utils
from discograph import decorators
from discograph.exceptions import BadRequestError, NotFoundError, DatabaseError
from discograph.library.cache.role_cache import RoleCache
from discograph.library.fields.entity_type import EntityType
from discograph.runtime.data_access_layer.runtime_entity_data_access import (
    RuntimeEntityDataAccess,
)
from discograph.runtime.runtime_database.runtime_entity_repository import (
    RuntimeEntityRepository,
)
from discograph.runtime.runtime_database.runtime_relation_repository import (
    RuntimeRelationRepository,
)
from discograph.runtime.runtime_database.runtime_transaction import runtime_transaction
from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager

log = logging.getLogger(__name__)

blueprint = Blueprint("api", __name__, template_folder="templates")


@blueprint.route("/<entity_type_str>/relations/<entity_id>")
@decorators.limit(max_requests=60, period=60)
def route__api__entity_type__relations__entity_id(entity_type_str, entity_id):
    # log.debug(f"entityType: {entity_type}")
    try:
        entity_type = EntityType.from_str(entity_type_str.upper())
    except NotImplementedError:
        raise BadRequestError(message="Bad Entity Type")
    if not entity_id.isnumeric():
        raise BadRequestError(message="Bad Entity Id")
    entity_id = int(entity_id)
    with runtime_transaction():
        entity_repository = RuntimeEntityRepository()
        relation_repository = RuntimeRelationRepository()
        # relation_release_year_repository = RuntimeRelationReleaseYearRepository()
        data = RuntimeDatabaseManager.runtime_database_helper.get_relations_by_entity_id_and_entity_type(
            entity_repository,
            relation_repository,
            # relation_release_year_repository,
            entity_id,
            entity_type,
        )
    if data is None:
        raise NotFoundError(message="No Data")
    return jsonify(data)


@blueprint.route("/<entity_type_str>/network/<entity_id>")
@decorators.limit(max_requests=60, period=60)
def route__api__entity_type__network__entity_id(entity_type_str, entity_id):
    # log.debug(f"entityType: {entity_type}")
    try:
        entity_type = EntityType.from_str(entity_type_str.upper())
    except NotImplementedError:
        raise BadRequestError(message="Bad Entity Type")
    # log.debug(f"entityType: {entity_type}")
    if not entity_id.isnumeric():
        raise BadRequestError(message="Bad Entity Id")
    entity_id = int(entity_id)
    # log.debug(f"entity_id: {entity_id}")
    parsed_args = discograph.utils.parse_request_args(request.args)
    original_roles, original_year = parsed_args
    # noinspection PyUnresolvedReferences
    # on_mobile = request.MOBILE
    with runtime_transaction():
        entity_repository = RuntimeEntityRepository()
        relation_repository = RuntimeRelationRepository()
        data = RuntimeDatabaseManager.runtime_database_helper.get_network(
            entity_repository,
            relation_repository,
            entity_id,
            entity_type,
            # on_mobile=on_mobile,
            roles=original_roles,
        )
    if data is None:
        raise NotFoundError(message="No Data")
    return jsonify(data)


@blueprint.route("/search/<search_string>")
@decorators.limit(max_requests=120, period=60)
def route__api__search(search_string):
    log.debug(f"search_string: {search_string}")
    data = RuntimeEntityDataAccess.search_entities(search_string)
    return jsonify(data)


@blueprint.route("/random")
@decorators.limit(max_requests=60, period=60)
def route__api__random():
    with runtime_transaction():
        entity_repository = RuntimeEntityRepository()
        try:
            entity_id, entity_type = (
                RuntimeDatabaseManager.runtime_database_helper.get_random_entity(
                    entity_repository
                )
            )
            log.debug(f"    Found random entity: {entity_type}-{entity_id}")
        except Exception:
            log.exception("Error in API for /random", exc_info=True)
            raise DatabaseError(message="API error")

    data = {"center": f"{entity_type.name.lower()}-{entity_id}"}
    return jsonify(data)


@blueprint.route("/roles")
@decorators.limit(max_requests=60, period=60)
def route__api__role():
    role_data = RoleCache.get_all_roles()
    return jsonify(role_data)
