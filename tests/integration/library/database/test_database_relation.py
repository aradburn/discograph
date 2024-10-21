from discograph import utils
from discograph.library.database.relation_repository import RelationRepository
from discograph.library.database.transaction import transaction
from discograph.library.domain.entity import Entity
from discograph.library.fields.entity_type import EntityType
from tests.integration.library.database.database_test_case import DatabaseTestCase


class TestDatabaseRelation(DatabaseTestCase):

    def test_from_db_01(self):
        # GIVEN
        entity_one_id = 42
        entity_one_type = EntityType.ARTIST
        entity_two_id = 41
        entity_two_type = EntityType.ARTIST

        # WHEN
        with transaction():
            relation_repository = RelationRepository()

            id_1 = Entity.to_entity_internal_id(entity_one_id, entity_one_type)
            id_2 = Entity.to_entity_internal_id(entity_two_id, entity_two_type)
            print(f"id_1: {id_1}")
            print(f"id_2: {id_2}")
            key = dict(
                subject=id_1,
                role="Producer",
                object=id_2,
            )

            relation = relation_repository.find_by_key(key)
            actual = utils.normalize_dict(relation.model_dump(exclude={"id", "random"}))

        # THEN
        expected_relation = {
            "subject": 42,
            "role": "Producer",
            "object": 41,
            # "releases": {
            #     "1000619": 2003,
            #     "1054046": None,
            #     "1099482": 2001,
            #     "11216": 1998,
            #     "1141130": None,
            #     "1169229": 2007,
            #     "1195788": 2005,
            #     "1203976": 1997,
            #     "1203978": 1993,
            #     "1257383": 2008,
            #     "1265276": 2008,
            #     "135858": 2002,
            #     "1377682": 1997,
            #     "13939": 1997,
            #     "1530077": 2002,
            #     "1530086": 2006,
            #     "157": 1994,
            #     "168639": 2003,
            #     "1741441": None,
            #     "1774969": 1999,
            #     "197416": 2003,
            #     "2009213": 1998,
            #     "2012450": 2006,
            #     "21351": 1999,
            #     "2188879": 2002,
            #     "2191313": 2010,
            #     "2276345": 2002,
            #     "2291695": 1994,
            #     "2317370": 2009,
            #     "239832": 1996,
            #     "2493": 1993,
            #     "2502": 1994,
            #     "251157": 2001,
            #     "25492": 1994,
            #     "26454": 1994,
            #     "26455": 1994,
            #     "2776338": 2001,
            #     "292020": 2004,
            #     "29372": 1992,
            #     "29373": 1992,
            #     "29900": 1993,
            #     "30187": 1994,
            #     "30188": 1994,
            #     "3066": 2001,
            #     "315067": 1992,
            #     "33098": 1995,
            #     "33099": 1996,
            #     "3392955": 2005,
            #     "34629": 2002,
            #     "3564784": 1992,
            #     "3674448": 1999,
            #     "36895": 2002,
            #     "383794": 1994,
            #     "398252": 1997,
            #     "4030071": 2012,
            #     "426323": 2005,
            #     "435111": 2005,
            #     "491787": 1997,
            #     "51735": 1994,
            #     "51766": 1994,
            #     "539874": 1995,
            #     "549": 1992,
            #     "571561": 1998,
            #     "66011": 1997,
            #     "708572": 1997,
            #     "752745": 1993,
            #     "790582": 1994,
            #     "793894": 1994,
            #     "81009": 1991,
            #     "826408": 2006,
            #     "870851": 2005,
            #     "8816": 1994,
            # },
        }

        expected = utils.normalize_dict(expected_relation)
        self.assertEqual(expected, actual)

    def test_from_db_02(self):
        # GIVEN
        entity_one_id = 21209
        entity_one_type = EntityType.ARTIST
        entity_two_id = 3771
        entity_two_type = EntityType.ARTIST

        # WHEN
        with transaction():
            relation_repository = RelationRepository()

            id_1 = Entity.to_entity_internal_id(entity_one_id, entity_one_type)
            id_2 = Entity.to_entity_internal_id(entity_two_id, entity_two_type)
            key = dict(
                subject=id_1,
                role="Compiled By",
                object=id_2,
            )

            relation = relation_repository.find_by_key(key)
            actual = utils.normalize_dict(relation.model_dump(exclude={"id", "random"}))

        # THEN
        expected_relation = {
            "subject": 21209,
            "role": "Compiled By",
            "object": 3771,
            # "releases": {
            #     "1112162": 1996,
            #     "1112454": 1996,
            #     "17268": 1994,
            #     "63148": 1994,
            # },
        }

        expected = utils.normalize_dict(expected_relation)
        self.assertEqual(expected, actual)

    def test_from_db_03(self):
        # GIVEN
        entity_one_id = 335173
        entity_one_type = EntityType.ARTIST
        entity_two_id = 41
        entity_two_type = EntityType.ARTIST

        # WHEN
        with transaction():
            relation_repository = RelationRepository()

            id_1 = Entity.to_entity_internal_id(entity_one_id, entity_one_type)
            id_2 = Entity.to_entity_internal_id(entity_two_id, entity_two_type)
            key = dict(
                subject=id_1,
                role="Mastered By",
                object=id_2,
            )

            relation = relation_repository.find_by_key(key)
            actual = utils.normalize_dict(relation.model_dump(exclude={"id", "random"}))

        expected_relation = {
            "subject": 335173,
            "role": "Mastered By",
            "object": 41,
            # "releases": {
            #     "1255104": 2008,
            #     "1257383": 2008,
            #     "1265276": 2008,
            #     "1265680": 2008,
            #     "130115": 2003,
            #     "130319": 2003,
            #     "156930": 2003,
            #     "2191313": 2010,
            #     "2192053": 2010,
            #     "2316777": None,
            #     "2346144": 2010,
            #     "2348913": 2010,
            #     "2351842": 2010,
            #     "2363967": 2010,
            #     "2513487": 2010,
            #     "3392955": 2005,
            #     "397637": 2003,
            #     "414646": 2005,
            #     "426323": 2005,
            #     "435111": 2005,
            #     "593584": 2003,
            #     "870851": 2005,
            # },
        }

        expected = utils.normalize_dict(expected_relation)
        self.assertEqual(expected, actual)
