import unittest

from discograph.library.data_access_layer.role_data_access import RoleDataAccess


class TestRoleDataAccess(unittest.TestCase):
    def test_normalise_01(self):
        input_str = "Mastered By"
        expected_str = "Mastered By"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_02a(self):
        input_str = "Mastered by"
        expected_str = "Mastered By"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_02b(self):
        input_str = "Mastered-by"
        expected_str = "Mastered By"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_03(self):
        input_str = "Tam-Tam"
        expected_str = "Tam-Tam"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_04(self):
        input_str = "Tam-tam"
        expected_str = "Tam-Tam"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_05(self):
        input_str = "tam-tam"
        expected_str = "Tam-Tam"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_06(self):
        input_str = "Tam-tam tam-Tam Tam-tam"
        expected_str = "Tam-Tam Tam-Tam Tam-Tam"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_07(self):
        input_str = "Oboe d\u0027Amore"
        expected_str = "Oboe d\u0027Amore"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_08(self):
        input_str = "Oboe D\u0027amore"
        expected_str = "Oboe d\u0027Amore"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_09(self):
        input_str = "MC"
        expected_str = "MC"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_10(self):
        input_str = "DAW"
        expected_str = "DAW"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_11(self):
        input_str = "DJ Mix"
        expected_str = "DJ Mix"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_12(self):
        input_str = "Tar (Lute)"
        expected_str = "Tar (Lute)"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_13(self):
        input_str = "Tar (lute)"
        expected_str = "Tar (Lute)"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_14(self):
        input_str = "Written-By"
        expected_str = "Written By"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_15(self):
        input_str = "Written By"
        expected_str = "Written By"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_16(self):
        input_str = "Written-by"
        expected_str = "Written By"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_17(self):
        input_str = "A&R"
        expected_str = "A&R"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_18(self):
        input_str = "A&r"
        expected_str = "A&R"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_19(self):
        input_str = "CGI Artist"
        expected_str = "CGI Artist"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_20(self):
        input_str = "Cgi Artist"
        expected_str = "CGI Artist"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_21(self):
        input_str = "DJ Mix"
        expected_str = "DJ Mix"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_22(self):
        input_str = "Dj mix"
        expected_str = "DJ Mix"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_23(self):
        input_str = "Vibes"
        expected_str = "Vibraphone"
        actual_str = RoleDataAccess.normalise_role_name(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_role_names_1(self):
        input_str = "Something and Something Else"
        expected_str = ["Something", "Something Else"]
        actual_str = RoleDataAccess.normalise_role_names(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_role_names_2(self):
        input_str = "Something & Something Else"
        expected_str = ["Something", "Something Else"]
        actual_str = RoleDataAccess.normalise_role_names(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_role_names_3(self):
        input_str = "Something and Something Else and More"
        expected_str = ["Something", "Something Else", "More"]
        actual_str = RoleDataAccess.normalise_role_names(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_role_names_4(self):
        input_str = "Something and Something Else and More & more"
        expected_str = ["Something", "Something Else", "More", "More"]
        actual_str = RoleDataAccess.normalise_role_names(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_role_names_5(self):
        input_str = "Written by & vibes"
        expected_str = ["Written By", "Vibraphone"]
        actual_str = RoleDataAccess.normalise_role_names(input_str)
        self.assertEqual(expected_str, actual_str)

    def test_normalise_role_names_6(self):
        input_str = "Artwork & Package Design By"
        expected_str = ["Artwork", "Package Design By"]
        actual_str = RoleDataAccess.normalise_role_names(input_str)
        self.assertEqual(expected_str, actual_str)
