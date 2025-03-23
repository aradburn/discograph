"""
This module defines the API endpoints for the Discograph application.

It provides Flask routes for handling requests related to entities,
relations, networks, search, and random entities. It also includes error
handling and rate limiting.

Key functionalities include:
    - Retrieving relations for a specific entity.
    - Retrieving the network graph for a specific entity.
    - Searching for entities based on a search string.
    - Getting a random entity.
    - Getting all available roles.
    - Handling bad requests, not found errors, and database errors.
    - Applying rate limiting to various endpoints.

The API endpoints interact with the runtime database through
`RuntimeEntityRepository` and `RuntimeRelationRepository`. They use
`runtime_transaction` to ensure database operations are performed within
transactions.

The module uses `discograph.utils` for request argument parsing and
`discograph.decorators` for rate limiting. `RoleCache` is used for retrieving
role information.
"""

import logging

from flask import Blueprint
from flask import jsonify
from flask import request

import discograph.utils
from discograph import decorators
from discograph.exceptions import BadRequestError, NotFoundError, DatabaseError
from discograph.library.cache.role_cache import RoleCache
from discograph.library.fields.entity_type import EntityType
from discograph.runtime.data_access_layer.runtime_entity_search import (
    RuntimeEntitySearch,
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
"""
The logger for this module.
"""

blueprint = Blueprint("api", __name__)
"""
The Flask blueprint for the API endpoints.

This blueprint is used to organize the API routes and their related functionality.
"""


@blueprint.route("/<entity_type_str>/relations/<entity_id>")
@decorators.limit(max_requests=60, period=60)
def route__api__entity_type__relations__entity_id(entity_type_str, entity_id):
    """
    Retrieves relations for a specific entity.

    This endpoint returns the relations associated with a given entity,
    identified by its type and ID.

    Args:
        entity_type_str (str): The type of the entity (e.g., "artist", "label").
        entity_id (str): The ID of the entity.

    Returns:
        flask.Response: A JSON response containing the relations data.

    Raises:
        BadRequestError: If the entity type or entity ID is invalid.
        NotFoundError: If no data is found for the given entity.
    """
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
    """
    Retrieves the network graph for a specific entity.

    This endpoint returns the network graph centered around a given entity,
    identified by its type and ID. It supports filtering by roles.

    Args:
        entity_type_str (str): The type of the entity (e.g., "artist", "label").
        entity_id (str): The ID of the entity.

    Returns:
        flask.Response: A JSON response containing the network graph data.

    Raises:
        BadRequestError: If the entity type or entity ID is invalid.
        NotFoundError: If no data is found for the given entity.
    """
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
    on_mobile = False
    with runtime_transaction():
        entity_repository = RuntimeEntityRepository()
        relation_repository = RuntimeRelationRepository()
        data = RuntimeDatabaseManager.runtime_database_helper.get_network(
            entity_repository,
            relation_repository,
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
    """
    Searches for entities based on a search string.

    This endpoint returns a list of entities that match the given search string.

    Args:
        search_string (str): The string to search for.

    Returns:
        flask.Response: A JSON response containing the search results.
    """
    log.debug(f"search_string: {search_string}")
    data = RuntimeEntitySearch.search_entities(search_string)
    return jsonify(data)


@blueprint.route("/random")
@decorators.limit(max_requests=60, period=60)
def route__api__random():
    """
    Retrieves a random entity.

    This endpoint returns a random entity from the database.

    Returns:
        flask.Response: A JSON response containing the random entity's type and ID.

    Raises:
        DatabaseError: If there is an error retrieving the random entity.
    """
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
    """
    Retrieves all available roles.

    This endpoint returns all the roles in the discograph application.

    Returns:
        flask.Response: A JSON response containing a list of all the roles.
    """
    role_data = RoleCache.get_all_roles()
    return jsonify(role_data)
