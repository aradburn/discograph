"""
This module defines the user interface (UI) routes for the Discograph application.

It handles requests for the main index page, entity-specific pages, and
static files like favicons. It uses Flask's `Blueprint` to organize the routes
and interact with the Discograph backend.

Key functionalities include:
    - Serving the main index page with initial data for the network graph
      and roles.
    - Serving entity-specific pages, displaying the network graph for a given
      entity.
    - Serving favicons and other static files.
    - Handling request argument parsing for roles and year.
    - Integrating with `RoleCache` for role data and `RuntimeDatabaseManager`
      for network data.
    - Using `RuntimeEntityRepository` and `RuntimeRelationRepository` for
      database interactions.
    - Employing `runtime_transaction` for database transactions.
    - Handling `BadRequestError` and `NotFoundError`.

The module uses `discograph.utils` for parsing request arguments,
`discograph.exceptions` for custom exceptions, and `discograph.library.cache.role_cache`
for role caching. It interacts with `discograph.runtime` for database operations.
"""

import json
import logging
import os

from flask import Blueprint, send_from_directory
from flask import current_app as app
from flask import make_response
from flask import render_template
from flask import request
from flask import url_for

import discograph.utils
from discograph.exceptions import BadRequestError, NotFoundError
from discograph.library.cache.role_cache import RoleCache
from discograph.library.fields.entity_type import EntityType
from discograph.runtime.data_access_layer.role_entry import RoleEntry
from discograph.runtime.runtime_database.runtime_entity_repository import (
    RuntimeEntityRepository,
)
from discograph.runtime.runtime_database.runtime_relation_repository import (
    RuntimeRelationRepository,
)
from discograph.runtime.runtime_database.runtime_transaction import runtime_transaction

log = logging.getLogger(__name__)
"""
The logger for the UI module.
"""

blueprint = Blueprint(
    "ui",
    __name__,
    static_url_path="/public/",
    static_folder="../public",
    template_folder="templates",
)
"""
The Flask blueprint for the UI routes.

This blueprint is used to organize the UI routes and their related functionality.
"""


UI_DEFAULT_ROLES = (
    "Alias",
    "Member Of",
    # 'Sublabel Of',
    # 'Released On',
)
"""
Default roles to display if none are specified in the request.
"""


@blueprint.route("/")
def route__index():
    """
    Serves the main index page.

    This route handles requests to the root URL ("/"). It prepares the
    initial data for the network graph and roles, renders the index
    template, and returns the response.

    Returns:
        flask.Response: The rendered index page.
    """
    network_js = "var dgNetwork = null;\n"
    """Initial JavaScript for the network graph, set to null."""
    log.debug(f"network_js: {network_js}")

    roles_json = RoleCache.get_roles_json()
    """Get the roles JSON data from the RoleCache."""
    roles_js = f"var dgRoles = {roles_json};\n"
    # log.debug(f"roles_js: {roles_js}")

    # Combines network and roles json
    initial_js = network_js + roles_js
    """Combine the network and roles JavaScript variables."""
    # log.debug(f"initial_js: {initial_js}")

    parsed_args = discograph.utils.parse_request_args(request.args)
    """Parse the request arguments for roles and year."""
    original_roles, original_year = parsed_args
    if not original_roles:
        original_roles = UI_DEFAULT_ROLES
    """Use default roles if none are specified in the request."""
    multiselect_mapping = RoleEntry.get_multiselect_mapping()
    """Get the multiselect mapping for roles."""
    url = url_for(
        request.endpoint,
        roles=original_roles,
    )
    """Generate the URL for the current request with the selected roles."""
    rendered_template = render_template(
        "index.html",
        application_url=app.config["APPLICATION_ROOT"],
        initial_json=initial_js,
        multiselect_mapping=multiselect_mapping,
        og_title="Discograph2",
        og_url=url,
        original_roles=original_roles,
        original_year=original_year,
        title="Discograph2",
    )
    """Render the index template with the prepared data."""
    response = make_response(rendered_template)
    """Create a Flask response object."""
    return response


@blueprint.route("/<entity_type_str>/<entity_id>")
def route__entity_type__entity_id(entity_type_str, entity_id):
    """
    Serves the entity-specific page.

    This route handles requests for URLs like "/artist/123" or "/label/456".
    It retrieves the network graph data for the specified entity, prepares
    the initial data, renders the index template, and returns the response.

    Args:
        entity_type_str (str): The type of the entity (e.g., "artist", "label").
        entity_id (str): The ID of the entity.

    Returns:
        flask.Response: The rendered entity-specific page.

    Raises:
        BadRequestError: If the entity type or entity ID is invalid.
        NotFoundError: If no network data is found for the given entity.
    """
    from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager

    parsed_args = discograph.utils.parse_request_args(request.args)
    """Parse the request arguments for roles and year."""
    requested_roles, requested_year = parsed_args
    if not requested_roles:
        requested_roles = UI_DEFAULT_ROLES
    """Use default roles if none are specified in the request."""
    try:
        entity_type = EntityType.from_str(entity_type_str.upper())
    except NotImplementedError:
        raise BadRequestError(message="Bad Entity Type")
    """Validate the entity type."""
    if not entity_id.isnumeric():
        raise BadRequestError(message="Bad Entity Id")
    """Validate the entity ID."""
    entity_id = int(entity_id)

    with runtime_transaction():
        entity_repository = RuntimeEntityRepository()
        relation_repository = RuntimeRelationRepository()
        network_data = RuntimeDatabaseManager.runtime_database_helper.get_network(
            entity_repository,
            relation_repository,
            entity_id,
            entity_type,
            on_mobile=False,
            roles=requested_roles,
        )
    """Retrieve the network data for the entity."""
    if network_data is None:
        raise NotFoundError(message="No Network Data")
    """Raise NotFoundError if no network data is found."""

    network_json = json.dumps(
        network_data,
        sort_keys=True,
        indent=4,
        separators=(",", ": "),
    )
    """Convert the network data to JSON."""
    network_js = f"var dgNetwork = {network_json};\n"
    """Create a JavaScript variable for the network data."""
    log.debug(f"network_js: {network_js}")

    roles_json = RoleCache.get_roles_json()
    """Get the roles JSON data from the RoleCache."""
    roles_js = f"var dgRoles = {roles_json};\n"
    # log.debug(f"roles_js: {roles_js}")

    # Combines network and roles json
    initial_js = network_js + roles_js
    """Combine the network and roles JavaScript variables."""
    # log.debug(f"initial_js: {initial_js}")

    entity_name = network_data["center"]["name"]
    """Extract the entity name from the network data."""
    key = f"{entity_type.name.lower()}-{entity_id}"
    """Create a unique key for the entity."""
    # url = '/{}/{}'.format(entity_type, entity_id)
    url = url_for(
        request.endpoint,
        entity_type_str=entity_type.name.lower(),
        entity_id=entity_id,
        roles=requested_roles,
    )
    """Generate the URL for the current entity."""
    title = f"Discograph2: {entity_name}"
    """Set the page title."""
    multiselect_mapping = RoleEntry.get_multiselect_mapping()
    """Get the multiselect mapping for roles."""
    rendered_template = render_template(
        "index.html",
        application_url=app.config["APPLICATION_ROOT"],
        initial_json=initial_js,
        key=key,
        multiselect_mapping=multiselect_mapping,
        og_title=f'Discograph2: The "{entity_name}" network',
        og_url=url,
        original_roles=requested_roles,
        original_year=requested_year,
        title=title,
    )
    """Render the index template with the prepared data."""
    response = make_response(rendered_template)
    """Create a Flask response object."""
    return response


@blueprint.route("/favicon.ico")
def favicon():
    """
    Serves the favicon.ico file.

    Returns:
        flask.Response: The favicon.ico file.
    """
    return send_from_directory(
        os.path.join(app.root_path, "static"),
        path="favicon.ico",
        mimetype="image/vnd.microsoft.icon",
    )


@blueprint.route("/favicon-32x32.png")
def favicon32():
    """
    Serves the favicon-32x32.png file.

    Returns:
        flask.Response: The favicon-32x32.png file.
    """
    return send_from_directory(
        os.path.join(app.root_path, "static"), path="favicon-32x32.png"
    )


@blueprint.route("/apple-touch-icon.png")
def favicon_apple():
    """
    Serves the apple-touch-icon.png file.

    Returns:
        flask.Response: The apple-touch-icon.png file.
    """
    return send_from_directory(
        os.path.join(app.root_path, "static"), path="apple-touch-icon.png"
    )
