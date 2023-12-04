import random

from discograph.library import EntityType, CreditRole
from discograph.library.database_helper import DatabaseHelper
from discograph.library.discogs_model import DiscogsModel
from discograph.library.sqlite.sqlite_entity import SqliteEntity
from discograph.library.sqlite.sqlite_relation import SqliteRelation
from discograph.library.sqlite.sqlite_relation_grapher import SqliteRelationGrapher


class SqliteHelper(DatabaseHelper):
    @staticmethod
    def get_entity(entity_type: EntityType, entity_id: int):
        assert entity_type in (EntityType.ARTIST, EntityType.LABEL)
        where_clause = SqliteEntity.entity_id == entity_id
        where_clause &= SqliteEntity.entity_type == entity_type
        with DiscogsModel.connection_context():
            query = SqliteEntity.select().where(where_clause)
            if not query.count():
                return None
            return query.get()

    @staticmethod
    def get_network(entity_id: int, entity_type: EntityType, on_mobile=False, cache=True, roles=None):
        print(f"entity_type: {entity_type}")
        assert entity_type in (EntityType.ARTIST, EntityType.LABEL)
        template = 'discograph:/api/{entity_type}/network/{entity_id}'
        if on_mobile:
            template = '{}/mobile'.format(template)

        cache_key_formatter = SqliteRelationGrapher.make_cache_key(
            template,
            entity_type,
            entity_id,
            roles=roles,
            )
        cache_key = cache_key_formatter.format(entity_type, entity_id)
        print(f"cache_key: {cache_key}")
        cache = False
        if cache:
            data = SqliteRelationGrapher.cache_get(cache_key)
            if data is not None:
                return data
        # entity_type = entity_name_types[entity_type]
        entity = SqliteHelper.get_entity(entity_type, entity_id)
        print(f"entity: {entity}")
        if entity is None:
            return None
        if not on_mobile:
            max_nodes = DatabaseHelper.MAX_NODES
            degree = DatabaseHelper.MAX_DEGREE
        else:
            max_nodes = DatabaseHelper.MAX_NODES_MOBILE
            degree = DatabaseHelper.MAX_DEGREE_MOBILE
        relation_grapher = SqliteRelationGrapher(
            center_entity=entity,
            degree=degree,
            max_nodes=max_nodes,
            roles=roles,
            )
        with DiscogsModel.connection_context():
            data = relation_grapher()
        if cache:
            SqliteRelationGrapher.cache_set(cache_key, data)
        return data

    @staticmethod
    def get_random_entity(roles=None):
        structural_roles = [
            'Alias',
            'Member Of',
            'Sublabel Of',
            ]
        with DiscogsModel.connection_context():
            if roles and any(_ not in structural_roles for _ in roles):
                relation = SqliteRelation.get_random(roles=roles)
                entity_choice = random.randint(1, 2)
                if entity_choice == 1:
                    entity_type = relation.entity_one_type
                    entity_id = relation.entity_one_id
                else:
                    entity_type = relation.entity_two_type
                    entity_id = relation.entity_two_id
            else:
                entity = SqliteEntity.get_random()
                entity_type, entity_id = entity.entity_type, entity.entity_id
        assert entity_type in (EntityType.ARTIST, EntityType.LABEL)
        return entity_type, entity_id

    @staticmethod
    def get_relations(entity_id: int, entity_type: EntityType):
        assert entity_type in (EntityType.ARTIST, EntityType.LABEL)
        # if isinstance(entity_type, str):
        #     entity_type = entity_name_types[entity_type]
        entity = SqliteHelper.get_entity(entity_type, entity_id)
        if entity is None:
            return None
        with DiscogsModel.connection_context():
            query = SqliteRelation.search(
                entity_id=entity.entity_id,
                entity_type=entity.entity_type,
                query_only=True
                )
        query = query.order_by(
            SqliteRelation.role,
            SqliteRelation.entity_one_id,
            SqliteRelation.entity_one_type,
            SqliteRelation.entity_two_id,
            SqliteRelation.entity_two_type,
            )
        data = []
        for relation in query:
            category = CreditRole.all_credit_roles[relation.role]
            if category is None:
                continue
            datum = {
                'role': relation.role,
                }
            data.append(datum)
        data = {'results': tuple(data)}
        return data

    @staticmethod
    def parse_request_args(args):
        from discograph.utils import args_roles_pattern
        year = None
        roles = set()
        for key in args:
            if key == 'year':
                year = args[key]
                try:
                    if '-' in year:
                        start, _, stop = year.partition('-')
                        year = tuple(sorted((int(start), int(stop))))
                    else:
                        year = int(year)
                finally:
                    pass
            elif args_roles_pattern.match(key):
                value = args.getlist(key)
                for role in value:
                    if role in CreditRole.all_credit_roles:
                        roles.add(role)
        roles = list(sorted(roles))
        return roles, year

    @staticmethod
    def search_entities(search_string, cache=True):
        from discograph.utils import urlify_pattern
        cache_key = 'discograph:/api/search/{}'.format(
            urlify_pattern.sub('+', search_string))
        cache = False
        if cache:
            data = SqliteRelationGrapher.cache_get(cache_key)
            if data is not None:
                print('{}: CACHED'.format(cache_key))
                for datum in data['results']:
                    print('    {}'.format(datum))
                return data
        with DiscogsModel.connection_context():
            query = SqliteEntity.search_text(search_string)
            print(f"query: {query}")
            print('{}: NOT CACHED'.format(cache_key))
            data = []
            for entity in query:
                datum = dict(
                    key='{}-{}'.format(
                        entity.entity_type.name.lower(),
                        entity.entity_id,
                        ),
                    name=entity.name,
                    )
                data.append(datum)
                print('    {}'.format(datum))
        data = {'results': tuple(data)}
        if cache:
            SqliteRelationGrapher.cache_set(cache_key, data)
        return data
