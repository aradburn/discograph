import unittest

from discograph.library.models.role import Role


class TestDatabaseRole(unittest.TestCase):
    def test_normalize_01(self):
        input_str = "Mastered By"
        expected_str = "Mastered By"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_02(self):
        input_str = "Mastered by"
        expected_str = "Mastered By"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_03(self):
        input_str = "Tam-Tam"
        expected_str = "Tam-Tam"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_04(self):
        input_str = "Tam-tam"
        expected_str = "Tam-Tam"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_05(self):
        input_str = "tam-tam"
        expected_str = "Tam-Tam"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_06(self):
        input_str = "Tam-tam tam-Tam Tam-tam"
        expected_str = "Tam-Tam Tam-Tam Tam-Tam"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_07(self):
        input_str = "Oboe d\u0027Amore"
        expected_str = "Oboe d\u0027Amore"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_08(self):
        input_str = "Oboe D\u0027amore"
        expected_str = "Oboe d\u0027Amore"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_09(self):
        input_str = "MC"
        expected_str = "MC"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_10(self):
        input_str = "DAW"
        expected_str = "DAW"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_11(self):
        input_str = "DJ Mix"
        expected_str = "DJ Mix"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_12(self):
        input_str = "Tar (Lute)"
        expected_str = "Tar (Lute)"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_13(self):
        input_str = "Tar (lute)"
        expected_str = "Tar (Lute)"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_14(self):
        input_str = "Written-By"
        expected_str = "Written By"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_15(self):
        input_str = "Written By"
        expected_str = "Written By"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_16(self):
        input_str = "Written-by"
        expected_str = "Written By"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_17(self):
        input_str = "A&R"
        expected_str = "A&R"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_18(self):
        input_str = "A&r"
        expected_str = "A&R"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_19(self):
        input_str = "CGI Artist"
        expected_str = "CGI Artist"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_20(self):
        input_str = "Cgi Artist"
        expected_str = "CGI Artist"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_21(self):
        input_str = "DJ Mix"
        expected_str = "DJ Mix"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_22(self):
        input_str = "Dj mix"
        expected_str = "DJ Mix"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalize_23(self):
        input_str = "Vibes"
        expected_str = "Vibraphone"
        actual_str = Role.normalize(input_str)
        self.assertEqual(expected_str, actual_str)
