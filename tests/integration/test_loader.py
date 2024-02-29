from discograph import database, utils
from discograph.library.entity_type import EntityType
from tests.integration.library.database.database_test_case import DatabaseTestCase
from tests.integration.loader_test_case import LoaderTestCase


class TestLoader(LoaderTestCase):
    def test_artist_record_updated(self):
        pk = (EntityType.ARTIST, 20702)
        entity = DatabaseTestCase.entity.get_by_id(pk)
        actual = utils.normalize(format(entity))
        expected_entity = {
            "entities": {"groups": {}},
            "entity_id": 20702,
            "entity_type": "EntityType.ARTIST",
            "metadata": {
                "name_variations": [
                    "L. Johnson",
                    "L. K. Johnson",
                    "L.K. Johnson",
                    "L.K.J.",
                    "LKJ",
                    'Linton "Kwesi" Johnson',
                    "Linton K. Johnson",
                    "Linton Kwesi-Johnson",
                    "Linton Kwisi Johnson",
                    "Linton Quasi Johnson",
                    "LKJ",
                ],
                "profile": "(Test Updated) Linton Kwesi Johnson (aka LKJ) (born 24 August 1952, "
                + "Chapelton, Jamaica) is a British-based author and dub poet. Johnson's "
                + "poetry makes clever use of the unstandardised transcription of Jamaican "
                + 'Patois and, allied to the Jamaican "toasting" tradition, is regarded as a '
                + "precursor of rap. He became the second living poet, and the only black poet, "
                + "to be published in the Penguin Classics series.",
                "urls": [
                    "http://www.lkjrecords.com/",
                    "http://lister.ultrakohl.com/homepage/Lkj/lkj.htm",
                    "http://en.wikipedia.org/wiki/Linton_Kwesi_Johnson",
                ],
            },
            "name": "Linton Kwesi Johnson",
        }
        expected = utils.normalize_dict(expected_entity)
        self.assertEqual(expected, actual)

    def test_artist_record_not_updated(self):
        pk = (EntityType.ARTIST, 2239)
        entity = DatabaseTestCase.entity.get_by_id(pk)
        actual = utils.normalize(format(entity))
        expected_entity = {
            "entities": {
                "members": {
                    "Daren Seymour": 66803,
                    "Justin Fletcher": 489350,
                    "Mark Clifford": 51674,
                    "Mark Van Hoen": 41103,
                    "Sarah Peacock": 115880,
                }
            },
            "entity_id": 2239,
            "entity_type": "EntityType.ARTIST",
            "metadata": {
                "profile": "British electronic/rock group formed in the early 1990s. "
                + "They are currently signed to Warp Records.",
                "real_name": "Sarah Peacock, Mark Clifford, Darren Seymour & Justin Fletcher",
                "urls": [
                    "http://www.myspace.com/seefeelmyspace",
                    "http://en.wikipedia.org/wiki/Seefeel",
                    "http://www.facebook.com/pages/Seefeel/146206372061290",
                    "http://twitter.com/#!/_Seefeel_",
                    "http://bit.ly/mQ9t3F",
                    "http://www.seefeel.org",
                ],
            },
            "name": "Seefeel",
        }
        expected = utils.normalize_dict(expected_entity)
        self.assertEqual(expected, actual)

    def test_label_record_updated(self):
        pk = (EntityType.LABEL, 1)
        entity = DatabaseTestCase.entity.get_by_id(pk)
        actual = utils.normalize(format(entity))
        expected_entity = {
            "entities": {},
            "entity_id": 1,
            "entity_type": "EntityType.LABEL",
            "metadata": {
                "profile": "(Test Update) Classic Techno label from Detroit, USA.\r\n[b]Label owner:[/b] [a=Carl Craig].\r\n",
                "urls": [
                    "http://www.planet-e.net/",
                    "http://www.myspace.com/planetecom",
                    "http://www.facebook.com/planetedetroit ",
                    "http://twitter.com/planetedetroit",
                    "http://soundcloud.com/planetedetroit",
                ],
            },
            "name": "Planet E (Test Update)",
        }
        expected = utils.normalize_dict(expected_entity)
        self.assertEqual(expected, actual)

    def test_label_record_not_updated(self):
        pk = (EntityType.LABEL, 264170)
        entity = DatabaseTestCase.entity.get_by_id(pk)
        actual = utils.normalize(format(entity))
        expected_entity = {
            "entities": {},
            "entity_id": 264170,
            "entity_type": "EntityType.LABEL",
            "metadata": {
                "profile": "American mastering studio located in New Windsor, NY. \r\n\r\n"
                + "Formally located at 2 Engle Street, Tenafly, New Jersey, "
                + "operations were moved to New Windsor in 2005. "
                + "Operated by Chief Engineer [a=Alan Douches].\n",
                "urls": ["http://www.westwestsidemusic.com/"],
            },
            "name": "West West Side Music",
        }
        expected = utils.normalize_dict(expected_entity)
        self.assertEqual(expected, actual)

    def test_release_updated(self):
        release_id = 157
        release = DatabaseTestCase.release.get_by_id(release_id)
        release.random = 0.0
        actual = utils.normalize(format(release))
        expected_release = {
            "artists": [{"id": 41, "name": "Autechre"}, {"id": 49, "name": "Gescom"}],
            "companies": [],
            "country": "UK",
            "extra_artists": [
                {
                    "id": 445854,
                    "name": "Designers Republic, The (Test Update)",
                    "roles": [{"name": "Design"}],
                },
                {
                    "anv": "Brown",
                    "id": 300407,
                    "name": "Rob Brown (3)",
                    "roles": [{"name": "Producer"}],
                },
                {
                    "anv": "Booth",
                    "id": 42,
                    "name": "Sean Booth",
                    "roles": [{"name": "Producer"}],
                },
            ],
            "formats": [
                {
                    "descriptions": ['12"', "EP", "33 \u2153 RPM", "45 RPM"],
                    "name": "Vinyl",
                    "quantity": "1",
                }
            ],
            "genres": ["Electronic", "(Test Update)"],
            "id": 157,
            "identifiers": [
                {"description": None, "type": "Barcode", "value": "5 021603 054066"},
                {
                    "description": "Etching A",
                    "type": "Matrix / Runout",
                    "value": "WAP-54-A\u2081 MA.",
                },
                {
                    "description": "Etching B",
                    "type": "Matrix / Runout",
                    "value": "WAP-54-B\u2081 MA.",
                },
            ],
            "labels": [
                {"catalog_number": "WAP54", "id": 23528, "name": "Warp Records"}
            ],
            "master_id": 1315,
            "notes": None,
            "random": 0.0,
            "release_date": "1994-09-03 00:00:00",
            "styles": ["Abstract", "IDM", "Experimental", "(Test Update)"],
            "title": "Anti EP",
            "tracklist": [
                {"position": "A1", "title": "Lost"},
                {"position": "A2", "title": "Djarum"},
                {"position": "B", "title": "Flutter"},
            ],
        }

        expected = utils.normalize_dict(expected_release)
        self.assertEqual(expected, actual)

    def test_release_not_updated(self):
        release_id = 635
        release = DatabaseTestCase.release.get_by_id(release_id)
        release.random = 0.0
        actual = utils.normalize(format(release))
        expected_release = {
            "artists": [{"id": 939, "name": "Higher Intelligence Agency, The"}],
            "companies": [],
            "country": "UK",
            "extra_artists": [
                {
                    "id": 939,
                    "name": "Higher Intelligence Agency, The",
                    "roles": [{"name": "Written-By"}],
                }
            ],
            "formats": [{"descriptions": ["EP"], "name": "CD", "quantity": "1"}],
            "genres": ["Electronic"],
            "id": 635,
            "identifiers": [
                {"description": None, "type": "Barcode", "value": "5 018524 066308"},
                {
                    "description": None,
                    "type": "Matrix / Runout",
                    "value": "DISCTRONICS S HIA 2 CD 01",
                },
            ],
            "labels": [{"catalog_number": "HIACD2", "id": 233, "name": "Beyond"}],
            "master_id": 21103,
            "notes": None,
            "random": 0.0,
            "release_date": "1994-01-01 00:00:00",
            "styles": ["Techno", "Ambient"],
            "title": "Colour Reform",
            "tracklist": [
                {
                    "duration": "8:49",
                    "extra_artists": [
                        {
                            "id": 932,
                            "name": "A Positive Life",
                            "roles": [{"name": "Remix"}],
                        }
                    ],
                    "position": "1",
                    "title": "Universal Entity (Ketamine Entity Reformed By A Positive Life)",
                },
                {
                    "duration": "6:24",
                    "extra_artists": [
                        {"id": 41, "name": "Autechre", "roles": [{"name": "Remix"}]}
                    ],
                    "position": "2",
                    "title": "Speech3 (Conoid Tone Reformed By Autechre)",
                },
                {
                    "duration": "8:30",
                    "extra_artists": [
                        {
                            "id": 379334,
                            "name": "Adrian Harrow",
                            "roles": [{"name": "Engineer"}],
                        },
                        {
                            "id": 953,
                            "name": "Irresistible Force, The",
                            "roles": [{"name": "Remix"}],
                        },
                    ],
                    "position": "3",
                    "title": "Speedlearn (Reformed By The Irresistible Force)",
                },
                {
                    "duration": "6:20",
                    "extra_artists": [
                        {"id": 954, "name": "Pentatonik", "roles": [{"name": "Remix"}]}
                    ],
                    "position": "4",
                    "title": "Alpha 1999 (Delta Reformed By Pentatonik)",
                },
            ],
        }

        expected = utils.normalize_dict(expected_release)
        self.assertEqual(expected, actual)

    def test_relation_not_updated_01(self):
        pk = (EntityType.ARTIST, 42, EntityType.ARTIST, 41, "Producer")
        relation = DatabaseTestCase.relation.get_by_id(pk)
        relation.random = 0.0
        actual = utils.normalize(format(relation))
        expected_relation = {
            "entity_one_id": 42,
            "entity_one_type": "EntityType.ARTIST",
            "entity_two_id": 41,
            "entity_two_type": "EntityType.ARTIST",
            "random": 0.0,
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

    def test_relation_not_updated_02(self):
        pk = (EntityType.ARTIST, 21209, EntityType.ARTIST, 3771, "Compiled By")
        relation = DatabaseTestCase.relation.get_by_id(pk)
        relation.random = 0.0
        actual = utils.normalize(format(relation))
        expected_relation = {
            "entity_one_id": 21209,
            "entity_one_type": "EntityType.ARTIST",
            "entity_two_id": 3771,
            "entity_two_type": "EntityType.ARTIST",
            "random": 0.0,
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

    def test_relation_not_updated_03(self):
        pk = (EntityType.ARTIST, 335173, EntityType.ARTIST, 41, "Mastered By")
        relation = DatabaseTestCase.relation.get_by_id(pk)
        relation.random = 0.0
        actual = utils.normalize(format(relation))
        expected_relation = {
            "entity_one_id": 335173,
            "entity_one_type": "EntityType.ARTIST",
            "entity_two_id": 41,
            "entity_two_type": "EntityType.ARTIST",
            "random": 0.0,
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
