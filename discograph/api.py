# -*- encoding: utf-8 -*-
from flask import Blueprint
from flask import request
from flask import jsonify

from discograph import decorators, SqliteRelation
from discograph import exceptions
from discograph import helpers
from discograph.helpers import entity_name_types

blueprint = Blueprint('api', __name__, template_folder='templates')


@blueprint.route('/<entity_type>/relations/<int:entity_id>')
# TODO AJR @decorators.limit(max_requests=60, period=60)
def route__api__entity_type__relations__entity_id(entity_type, entity_id):
    if entity_type not in (SqliteRelation.EntityType.ARTIST, SqliteRelation.EntityType.LABEL):
        raise exceptions.APIError(message='Bad Entity Type', status_code=404)
    data = helpers.get_relations(
        entity_id,
        entity_type,
        )
    if data is None:
        raise exceptions.APIError(message='No Data', status_code=400)
    return jsonify(data)


@blueprint.route('/<entity_type>/network/<int:entity_id>')
# TODO AJR @decorators.limit(max_requests=60, period=60)
def route__api__entity_type__network__entity_id(entity_type, entity_id):
    print(f"entityType: {entity_type}")
    entity_type = entity_name_types[entity_type]
    if entity_type not in (SqliteRelation.EntityType.ARTIST, SqliteRelation.EntityType.LABEL):
        raise exceptions.APIError(message='Bad Entity Type', status_code=404)
    parsed_args = helpers.parse_request_args(request.args)
    original_roles, original_year = parsed_args
    # TODO AJR on_mobile = request.MOBILE
    data = helpers.get_network(
        entity_id,
        entity_type,
        # TODO AJR on_mobile=on_mobile,
        cache=True,
        roles=original_roles,
        )
    if data is None:
        raise exceptions.APIError(message='No Data', status_code=400)
    return jsonify(data)


@blueprint.route('/search/<search_string>')
# TODO AJR @decorators.limit(max_requests=120, period=60)
def route__api__search(search_string):
    data = helpers.search_entities(search_string)
    return jsonify(data)


@blueprint.route('/random')
# TODO AJR @decorators.limit(max_requests=60, period=60)
def route__api__random():
    parsed_args = helpers.parse_request_args(request.args)
    original_roles, original_year = parsed_args
    print('Roles:', original_roles)
    entity_type, entity_id = helpers.get_random_entity(
        roles=original_roles,
        )
    print('    Found: {}-{}'.format(entity_type, entity_id))
    entity_type = {
        1: 'artist',
        2: 'label',
        }[entity_type.value]
    data = {'center': '{}-{}'.format(entity_type, entity_id)}
    return jsonify(data)
