from discograph import utils
from discograph.library.fields.entity_type import EntityType
from tests.integration.library.database.database_test_case import DatabaseTestCase


class TestDatabaseRelation(DatabaseTestCase):
    def test_from_db_01(self):
        pk = (42, EntityType.ARTIST, 41, EntityType.ARTIST, "Producer")
        with self.test_session.begin() as session:
            relation = session.get(DatabaseTestCase.relation, pk)
            actual = utils.normalize(format(relation))

        expected_relation = {
            "entity_one_id": 42,
            "entity_one_type": "EntityType.ARTIST",
            "entity_two_id": 41,
            "entity_two_type": "EntityType.ARTIST",
            "releases": {
                "1000619": 2003,
                "1054046": None,
                "1099482": 2001,
                "11216": 1998,
                "1141130": None,
                "1169229": 2007,
                "1195788": 2005,
                "1203976": 1997,
                "1203978": 1993,
                "1257383": 2008,
                "1265276": 2008,
                "135858": 2002,
                "1377682": 1997,
                "13939": 1997,
                "1530077": 2002,
                "1530086": 2006,
                "157": 1994,
                "168639": 2003,
                "1741441": None,
                "1774969": 1999,
                "197416": 2003,
                "2009213": 1998,
                "2012450": 2006,
                "21351": 1999,
                "2188879": 2002,
                "2191313": 2010,
                "2276345": 2002,
                "2291695": 1994,
                "2317370": 2009,
                "239832": 1996,
                "2493": 1993,
                "2502": 1994,
                "251157": 2001,
                "25492": 1994,
                "26454": 1994,
                "26455": 1994,
                "2776338": 2001,
                "292020": 2004,
                "29372": 1992,
                "29373": 1992,
                "29900": 1993,
                "30187": 1994,
                "30188": 1994,
                "3066": 2001,
                "315067": 1992,
                "33098": 1995,
                "33099": 1996,
                "3392955": 2005,
                "34629": 2002,
                "3564784": 1992,
                "3674448": 1999,
                "36895": 2002,
                "383794": 1994,
                "398252": 1997,
                "4030071": 2012,
                "426323": 2005,
                "435111": 2005,
                "491787": 1997,
                "51735": 1994,
                "51766": 1994,
                "539874": 1995,
                "549": 1992,
                "571561": 1998,
                "66011": 1997,
                "708572": 1997,
                "752745": 1993,
                "790582": 1994,
                "793894": 1994,
                "81009": 1991,
                "826408": 2006,
                "870851": 2005,
                "8816": 1994,
            },
            "role": "Producer",
        }

        expected = utils.normalize_dict(expected_relation)
        self.assertEqual(expected, actual)

    def test_from_db_02(self):
        pk = (21209, EntityType.ARTIST, 3771, EntityType.ARTIST, "Compiled By")
        with self.test_session.begin() as session:
            relation = session.get(DatabaseTestCase.relation, pk)
            actual = utils.normalize(format(relation))

        expected_relation = {
            "entity_one_id": 21209,
            "entity_one_type": "EntityType.ARTIST",
            "entity_two_id": 3771,
            "entity_two_type": "EntityType.ARTIST",
            "releases": {
                "1112162": 1996,
                "1112454": 1996,
                "17268": 1994,
                "63148": 1994,
            },
            "role": "Compiled By",
        }

        expected = utils.normalize_dict(expected_relation)
        self.assertEqual(expected, actual)

    def test_from_db_03(self):
        pk = (335173, EntityType.ARTIST, 41, EntityType.ARTIST, "Mastered By")
        with self.test_session.begin() as session:
            relation = session.get(DatabaseTestCase.relation, pk)
            actual = utils.normalize(format(relation))

        expected_relation = {
            "entity_one_id": 335173,
            "entity_one_type": "EntityType.ARTIST",
            "entity_two_id": 41,
            "entity_two_type": "EntityType.ARTIST",
            "releases": {
                "1255104": 2008,
                "1257383": 2008,
                "1265276": 2008,
                "1265680": 2008,
                "130115": 2003,
                "130319": 2003,
                "156930": 2003,
                "2191313": 2010,
                "2192053": 2010,
                "2316777": None,
                "2346144": 2010,
                "2348913": 2010,
                "2351842": 2010,
                "2363967": 2010,
                "2513487": 2010,
                "3392955": 2005,
                "397637": 2003,
                "414646": 2005,
                "426323": 2005,
                "435111": 2005,
                "593584": 2003,
                "870851": 2005,
            },
            "role": "Mastered By",
        }

        expected = utils.normalize_dict(expected_relation)
        self.assertEqual(expected, actual)
