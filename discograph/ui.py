import json

from flask import Blueprint
from flask import current_app as app
from flask import make_response
from flask import render_template
from flask import request
from flask import url_for

import discograph.utils
from discograph import database, exceptions
from discograph.library import CreditRole, EntityType

blueprint = Blueprint('ui', __name__, template_folder='templates')


default_roles = (
    'Alias',
    'Member Of',
    # 'Sublabel Of',
    # 'Released On',
)


@blueprint.route('/')
def route__index():
    is_a_return_visitor = request.cookies.get('is_a_return_visitor')
    initial_json = 'var dgData = null;'
    # noinspection PyUnresolvedReferences
    on_mobile = request.MOBILE
    parsed_args = discograph.utils.parse_request_args(request.args)
    original_roles, original_year = parsed_args
    if not original_roles:
        original_roles = default_roles
    multiselect_mapping = CreditRole.get_multiselect_mapping()
    url = url_for(
        request.endpoint,
        roles=original_roles,
        )
    rendered_template = render_template(
        'index.html',
        application_url=app.config['APPLICATION_ROOT'],
        initial_json=initial_json,
        is_a_return_visitor=is_a_return_visitor,
        multiselect_mapping=multiselect_mapping,
        og_title='DiscoGraph2',
        og_url=url,
        on_mobile=on_mobile,
        original_roles=original_roles,
        original_year=original_year,
        title='DiscoGraph2',
        )
    response = make_response(rendered_template)
    response.set_cookie('is_a_return_visitor', 'true')
    return response


@blueprint.route('/<entity_type>/<entity_id>')
def route__entity_type__entity_id(entity_type, entity_id):
    parsed_args = discograph.utils.parse_request_args(request.args)
    original_roles, original_year = parsed_args
    if not original_roles:
        original_roles = default_roles
    entity_type = EntityType[entity_type.upper()]
    if entity_type not in (EntityType.ARTIST, EntityType.LABEL):
        raise exceptions.APIError(message='Bad Entity Type', status_code=400)
    if not entity_id.isnumeric():
        raise exceptions.APIError(message='Bad Entity Id', status_code=400)
    entity_id = int(entity_id)

    # on_mobile = request.MOBILE
    data = database.db_helper.get_network(
        entity_id,
        entity_type,
        # on_mobile=on_mobile,
        cache=True,
        roles=original_roles,
        )
    if data is None:
        raise exceptions.APIError(message='No Data', status_code=404)
    initial_json = json.dumps(
        data,
        sort_keys=True,
        indent=4,
        separators=(',', ': '),
        )
    initial_json = 'var dgData = {};'.format(initial_json)
    entity_name = data['center']['name']
    is_a_return_visitor = request.cookies.get('is_a_return_visitor')
    key = '{}-{}'.format(entity_type.name.lower(), entity_id)
    # url = '/{}/{}'.format(entity_type, entity_id)
    url = url_for(
        request.endpoint,
        entity_type=entity_type.name.lower(),
        entity_id=entity_id,
        roles=original_roles,
        )
    title = 'Discograph2: {}'.format(entity_name)
    multiselect_mapping = CreditRole.get_multiselect_mapping()
    rendered_template = render_template(
        'index.html',
        application_url=app.config['APPLICATION_ROOT'],
        initial_json=initial_json,
        is_a_return_visitor=is_a_return_visitor,
        key=key,
        multiselect_mapping=multiselect_mapping,
        og_title='Discograph2: The "{}" network'.format(entity_name),
        og_url=url,
        # on_mobile=on_mobile,
        original_roles=original_roles,
        original_year=original_year,
        title=title,
        )
    response = make_response(rendered_template)
    response.set_cookie('is_a_return_visitor', 'true')
    return response
