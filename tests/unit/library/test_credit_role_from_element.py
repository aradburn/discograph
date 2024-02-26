import unittest
from xml.etree import ElementTree

from discograph.library.credit_role import CreditRole


class TestCreditRoleFromElement(unittest.TestCase):
    def test_1(self):
        element = ElementTree.fromstring("<test></test>")
        element.text = (
            "Shekere [Xequere, Original Musician], Guiro [Original Musician], "
            "Claves [Original Musician]"
        )
        roles = CreditRole.from_element(element)
        self.assertListEqual(
            roles,
            [
                CreditRole(name="Shekere", detail="Xequere, Original Musician"),
                CreditRole(name="Guiro", detail="Original Musician"),
                CreditRole(name="Claves", detail="Original Musician"),
            ],
        )

    def test_2(self):
        element = ElementTree.fromstring("<test></test>")
        element.text = (
            "Co-producer, Arranged By, Directed By, Other [Guided By], "
            "Other [Created By]"
        )
        roles = CreditRole.from_element(element)
        assert roles == [
            CreditRole(name="Co-producer"),
            CreditRole(name="Arranged By"),
            CreditRole(name="Directed By"),
            CreditRole(name="Other", detail="Guided By"),
            CreditRole(name="Other", detail="Created By"),
        ]

    def test_3(self):
        element = ElementTree.fromstring("<test></test>")
        element.text = (
            "Organ [Original Musician], " "Electric Piano [Rhodes, Original Musician]"
        )
        roles = CreditRole.from_element(element)
        assert roles == [
            CreditRole(
                name="Organ",
                detail="Original Musician",
            ),
            CreditRole(
                name="Electric Piano",
                detail="Rhodes, Original Musician",
            ),
        ]

    def test_4(self):
        element = ElementTree.fromstring("<test></test>")
        element.text = "Photography By ['hats' And 'spray' Photos By]"
        roles = CreditRole.from_element(element)
        assert roles == [
            CreditRole(
                name="Photography By",
                detail="'hats' And 'spray' Photos By",
            ),
        ]

    def test_5(self):
        element = ElementTree.fromstring("<test></test>")
        element.text = "Strings "
        roles = CreditRole.from_element(element)
        assert roles == [
            CreditRole(name="Strings"),
        ]

    def test_6(self):
        element = ElementTree.fromstring("<test></test>")
        element.text = "Piano, Synthesizer [Moog], Programmed By"
        roles = CreditRole.from_element(element)
        assert roles == [
            CreditRole(name="Piano"),
            CreditRole(name="Synthesizer", detail="Moog"),
            CreditRole(name="Programmed By"),
        ]

    def test_7(self):
        element = ElementTree.fromstring("<test></test>")
        element.text = "Percussion [Misc.]"
        roles = CreditRole.from_element(element)
        assert roles == [
            CreditRole(name="Percussion", detail="Misc."),
        ]

    def test_8(self):
        element = ElementTree.fromstring("<test></test>")
        element.text = 'Painting [Uncredited; Detail Of <i>"The Transfiguration"</i>]'
        roles = CreditRole.from_element(element)
        assert roles == [
            CreditRole(
                name="Painting",
                detail='Uncredited; Detail Of <i>"The Transfiguration"</i>',
            ),
        ]

    def test_9(self):
        element = ElementTree.fromstring("<test></test>")
        element.text = "Composed By, Words By [elemented By], Producer"
        roles = CreditRole.from_element(element)
        assert roles == [
            CreditRole(name="Composed By"),
            CreditRole(name="Words By", detail="elemented By"),
            CreditRole(name="Producer"),
        ]

    def test_10(self):
        element = ElementTree.fromstring("<test></test>")
        element.text = "Engineer [Remix] [Assistant], Producer"
        roles = CreditRole.from_element(element)
        assert roles == [
            CreditRole(name="Engineer", detail="Remix, Assistant"),
            CreditRole(name="Producer"),
        ]

    def test_11(self):
        element = ElementTree.fromstring("<test></test>")
        element.text = "Performer [Enigmatic [K] Voice, Moog, Korg Vocoder], Lyrics By"
        roles = CreditRole.from_element(element)
        assert roles == [
            CreditRole(
                name="Performer", detail="Enigmatic [K] Voice, Moog, Korg Vocoder"
            ),
            CreditRole(name="Lyrics By"),
        ]
