from discograph import utils
from discograph.library.fields.entity_type import EntityType
from discograph.library.loader_utils import LoaderUtils
from tests.integration.library.database.database_test_case import DatabaseTestCase


class TestDatabaseEntity(DatabaseTestCase):
    def test_from_element_01(self):
        iterator = LoaderUtils.get_iterator("artist", "test")
        element = next(iterator)
        entity = DatabaseTestCase.entity.from_element(element)
        actual = utils.normalize(format(entity))
        expected_entity = {
            "entities": {"aliases": {}, "groups": {}},
            "entity_id": 3,
            "entity_type": "EntityType.ARTIST",
            "metadata": {
                "name_variations": [
                    "DJ Josh Wink",
                    "DJ Wink",
                    "J Wink",
                    "J. Wink",
                    "J. Wink (DJ  Wink)",
                    "J. Winkelman",
                    "J. Winkelmann",
                    "J.Wink",
                    "J.Winkelman",
                    "Josh",
                    "Josh Wink (DJ Wink)",
                    "Josh Wink Aka Winx",
                    "Josh Winkelman",
                    "Josh Winkelmann",
                    "Josh Winx",
                    "JW",
                    "Unknown Artist",
                    "Winc",
                    "Wink",
                    "Wink (Feat The Interpreters)",
                    "Winks",
                    "Winx",
                    "Winxs",
                ],
                "profile": "After forming [l=Ovum Recordings] as an independent label in October 1994 "
                + "with former partner [a=King Britt], Josh recorded the cult classic 'Liquid Summer'. "
                + "He went on to release singles for a wide variety of revered European labels ranging "
                + "from Belgium's [l=R & S Records] to England's [l=XL Recordings]. In 1995, Wink became "
                + "one of the first DJ-producers to translate his hard work into mainstream success when "
                + "he unleashed a string of classics including 'Don't Laugh'\u00b8 'I'm Ready' and "
                + "'Higher State of Consciousness' that topped charts worldwide. "
                + "More recently he has had massive club hits such as 'How's Your Evening So Far' and "
                + "'Superfreak' but he has also gained a lot of attention trough his remixes for "
                + "[a=FC Kahuna], [a=Paul Oakenfold], [a=Ladytron], [a=Clint Mansell], [a=Sting] "
                + "and [a=Depeche Mode], among others.",
                "real_name": "Joshua Winkelman",
                "urls": [
                    "http://www.joshwink.com/",
                    "http://www.ovum-rec.com/",
                    "http://www.myspace.com/joshwink",
                    "http://www.myspace.com/ovumrecordings",
                    "http://www.deejaybooking.com/joshwink",
                    "http://twitter.com/joshwink1",
                ],
            },
            "name": "Josh Wink",
        }
        expected = utils.normalize_dict(expected_entity)
        self.assertEqual(expected, actual)

    def test_from_element_02(self):
        iterator = LoaderUtils.get_iterator("artist", "test")
        element = next(iterator)
        while element.find("name").text != "Seefeel":
            element = next(iterator)
        entity = DatabaseTestCase.entity.from_element(element)
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

    def test_from_element_03(self):
        iterator = LoaderUtils.get_iterator("label", "test")
        element = next(iterator)
        entity = DatabaseTestCase.entity.from_element(element)
        actual = utils.normalize(format(entity))
        expected_entity = {
            "entities": {},
            "entity_id": 1,
            "entity_type": "EntityType.LABEL",
            "metadata": {
                "profile": "Classic Techno label from Detroit, USA.\r\n[b]Label owner:[/b] [a=Carl Craig].\r\n",
                "urls": [
                    "http://www.planet-e.net/",
                    "http://www.myspace.com/planetecom",
                    "http://www.facebook.com/planetedetroit ",
                    "http://twitter.com/planetedetroit",
                    "http://soundcloud.com/planetedetroit",
                ],
            },
            "name": "Planet E",
        }
        expected = utils.normalize_dict(expected_entity)
        self.assertEqual(expected, actual)

    def test_from_db_01(self):
        pk = (EntityType.ARTIST, 3)
        entity = DatabaseTestCase.entity.get_by_id(pk)
        actual = utils.normalize(format(entity))
        expected_entity = {
            "entities": {"aliases": {}, "groups": {}},
            "entity_id": 3,
            "entity_type": "EntityType.ARTIST",
            "metadata": {
                "name_variations": [
                    "DJ Josh Wink",
                    "DJ Wink",
                    "J Wink",
                    "J. Wink",
                    "J. Wink (DJ  Wink)",
                    "J. Winkelman",
                    "J. Winkelmann",
                    "J.Wink",
                    "J.Winkelman",
                    "Josh",
                    "Josh Wink (DJ Wink)",
                    "Josh Wink Aka Winx",
                    "Josh Winkelman",
                    "Josh Winkelmann",
                    "Josh Winx",
                    "JW",
                    "Unknown Artist",
                    "Winc",
                    "Wink",
                    "Wink (Feat The Interpreters)",
                    "Winks",
                    "Winx",
                    "Winxs",
                ],
                "profile": "After forming [l=Ovum Recordings] as an independent label in October 1994 "
                + "with former partner [a=King Britt], Josh recorded the cult classic 'Liquid Summer'. "
                + "He went on to release singles for a wide variety of revered European labels ranging "
                + "from Belgium's [l=R & S Records] to England's [l=XL Recordings]. In 1995, Wink became "
                + "one of the first DJ-producers to translate his hard work into mainstream success when "
                + "he unleashed a string of classics including 'Don't Laugh'\u00b8 'I'm Ready' and "
                + "'Higher State of Consciousness' that topped charts worldwide. "
                + "More recently he has had massive club hits such as 'How's Your Evening So Far' and "
                + "'Superfreak' but he has also gained a lot of attention trough his remixes for "
                + "[a=FC Kahuna], [a=Paul Oakenfold], [a=Ladytron], [a=Clint Mansell], [a=Sting] "
                + "and [a=Depeche Mode], among others.",
                "real_name": "Joshua Winkelman",
                "urls": [
                    "http://www.joshwink.com/",
                    "http://www.ovum-rec.com/",
                    "http://www.myspace.com/joshwink",
                    "http://www.myspace.com/ovumrecordings",
                    "http://www.deejaybooking.com/joshwink",
                    "http://twitter.com/joshwink1",
                ],
            },
            "name": "Josh Wink",
        }
        expected = utils.normalize_dict(expected_entity)
        self.assertEqual(expected, actual)

    def test_from_db_02(self):
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

    def test_from_db_03(self):
        pk = (EntityType.LABEL, 1)
        entity = DatabaseTestCase.entity.get_by_id(pk)
        actual = utils.normalize(format(entity))
        expected_entity = {
            "entities": {},
            "entity_id": 1,
            "entity_type": "EntityType.LABEL",
            "metadata": {
                "profile": "Classic Techno label from Detroit, USA.\r\n[b]Label owner:[/b] [a=Carl Craig].\r\n",
                "urls": [
                    "http://www.planet-e.net/",
                    "http://www.myspace.com/planetecom",
                    "http://www.facebook.com/planetedetroit ",
                    "http://twitter.com/planetedetroit",
                    "http://soundcloud.com/planetedetroit",
                ],
            },
            "name": "Planet E",
        }
        expected = utils.normalize_dict(expected_entity)
        self.assertEqual(expected, actual)