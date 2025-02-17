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
from discograph.runtime.runtime_database.runtime_transaction import transaction
from discograph.runtime.runtime_domain.role import RuntimeRoleJSTreeWrapper

log = logging.getLogger(__name__)

blueprint = Blueprint("ui", __name__, template_folder="templates")


UI_DEFAULT_ROLES = (
    "Alias",
    "Member Of",
    # 'Sublabel Of',
    # 'Released On',
)


@blueprint.route("/")
def route__index():
    initial_json = "var dgNetwork = null; var dgRoles = null;"
    parsed_args = discograph.utils.parse_request_args(request.args)
    original_roles, original_year = parsed_args
    if not original_roles:
        original_roles = UI_DEFAULT_ROLES
    multiselect_mapping = RoleEntry.get_multiselect_mapping()
    url = url_for(
        request.endpoint,
        roles=original_roles,
    )
    rendered_template = render_template(
        "index.html",
        application_url=app.config["APPLICATION_ROOT"],
        initial_json=initial_json,
        multiselect_mapping=multiselect_mapping,
        og_title="Discograph2",
        og_url=url,
        original_roles=original_roles,
        original_year=original_year,
        title="Discograph2",
    )
    response = make_response(rendered_template)
    return response


@blueprint.route("/<entity_type_str>/<entity_id>")
def route__entity_type__entity_id(entity_type_str, entity_id):
    from discograph.runtime.runtime_database_manager import RuntimeDatabaseManager

    parsed_args = discograph.utils.parse_request_args(request.args)
    requested_roles, requested_year = parsed_args
    if not requested_roles:
        requested_roles = UI_DEFAULT_ROLES
    try:
        entity_type = EntityType.from_str(entity_type_str.upper())
    except NotImplementedError:
        raise BadRequestError(message="Bad Entity Type")
    if not entity_id.isnumeric():
        raise BadRequestError(message="Bad Entity Id")
    entity_id = int(entity_id)

    with transaction():
        entity_repository = RuntimeEntityRepository()
        relation_repository = RuntimeRelationRepository()
        network_data = RuntimeDatabaseManager.runtime_db_helper.get_network(
            entity_repository,
            relation_repository,
            entity_id,
            entity_type,
            roles=requested_roles,
        )
    if network_data is None:
        raise NotFoundError(message="No Network Data")
    network_json = json.dumps(
        network_data,
        sort_keys=True,
        indent=4,
        separators=(",", ": "),
    )

    roles_data = RuntimeRoleJSTreeWrapper(
        core=RoleCache.role_jstree,
        checkbox={"keep_selected_style": False},
        plugins=["checkbox"],
    )
    roles_json = roles_data.model_dump_json()
    initial_json = (
        f"var dgNetwork = {network_json};\n" + f"var dgRoles = {roles_json};\n"
    )
    # log.debug(f"initial_json: {initial_json}")

    entity_name = network_data["center"]["name"]
    key = f"{entity_type.name.lower()}-{entity_id}"
    # url = '/{}/{}'.format(entity_type, entity_id)
    url = url_for(
        request.endpoint,
        entity_type_str=entity_type.name.lower(),
        entity_id=entity_id,
        roles=requested_roles,
    )
    title = f"Discograph2: {entity_name}"
    multiselect_mapping = RoleEntry.get_multiselect_mapping()
    rendered_template = render_template(
        "index.html",
        application_url=app.config["APPLICATION_ROOT"],
        initial_json=initial_json,
        key=key,
        multiselect_mapping=multiselect_mapping,
        og_title=f'Discograph2: The "{entity_name}" network',
        og_url=url,
        original_roles=requested_roles,
        original_year=requested_year,
        title=title,
    )
    response = make_response(rendered_template)
    return response


@blueprint.route("/favicon.ico")
def favicon():
    return send_from_directory(
        os.path.join(app.root_path, "static"),
        path="favicon.ico",
        mimetype="image/vnd.microsoft.icon",
    )


@blueprint.route("/favicon-32x32.png")
def favicon32():
    return send_from_directory(
        os.path.join(app.root_path, "static"), path="favicon-32x32.png"
    )


@blueprint.route("/apple-touch-icon.png")
def favicon_apple():
    return send_from_directory(
        os.path.join(app.root_path, "static"), path="apple-touch-icon.png"
    )
