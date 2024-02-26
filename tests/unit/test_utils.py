import unittest

from discograph import utils


class TestUtils(unittest.TestCase):
    def test_split_tuple_1(self):
        input_seq = (1, 2, 3, 4, 10, 11, 12, 13, 20, 21, 22, 23)
        num_chunks = 3
        result = tuple(utils.split_tuple(num_chunks, input_seq))
        assert result == ((1, 2, 3, 4), (10, 11, 12, 13), (20, 21, 22, 23))

    def test_split_tuple_2(self):
        input_seq = (1, 2, 3, 4, 10, 11, 12, 13, 20, 21, 22, 23, 24)
        num_chunks = 3
        result = tuple(utils.split_tuple(num_chunks, input_seq))
        assert result == ((1, 2, 3, 4, 10), (11, 12, 13, 20, 21), (22, 23, 24))
        assert len(result) == num_chunks

    def test_split_tuple_3(self):
        input_seq = (1,)
        num_chunks = 3
        result = tuple(utils.split_tuple(num_chunks, input_seq))
        assert result == ((1,),)
        assert len(result) == 1

    def test_split_tuple_4(self):
        input_seq = ()
        num_chunks = 3
        result = tuple(utils.split_tuple(num_chunks, input_seq))
        assert result == ()
        assert len(result) == 0

    def test_split_tuple_5(self):
        input_seq = (1, 2, 3, 4, 10, 11, 12, 13, 20, 21, 22, 23)
        num_chunks = 0
        result = tuple(utils.split_tuple(num_chunks, input_seq))
        assert result == ((1, 2, 3, 4, 10, 11, 12, 13, 20, 21, 22, 23),)
        assert len(result) == 1

    def test_strip_input(self):
        input_str = """
            aaa
            bbb
            ccc
        """

        actual = utils.strip_input(input_str)
        expected = "aaa\nbbb\nccc\n"
        self.assertEqual(expected, actual)

    def test_normalize_dict(self):
        input_dict = {
            "entity_one_id": 430141,
            "entity_one_type": "EntityType.ARTIST",
            "entity_two_id": 307,
            "entity_two_type": "EntityType.ARTIST",
            "random": None,
            "releases": None,
            "role": "Member Of",
        }

        actual = utils.normalize_dict(input_dict)
        expected = """
            {
                "entity_one_id": 430141,
                "entity_one_type": "EntityType.ARTIST",
                "entity_two_id": 307,
                "entity_two_type": "EntityType.ARTIST",
                "random": null,
                "releases": null,
                "role": "Member Of"
            }
        """
        self.assertEqual(utils.strip_input(expected), actual)

    def test_normalize_dict_list(self):
        input_list = [
            {
                "entity_one_id": 430141,
                "entity_one_type": "EntityType.ARTIST",
                "entity_two_id": 307,
                "entity_two_type": "EntityType.ARTIST",
                "random": None,
                "releases": None,
                "role": "Member Of",
            },
            {
                "entity_one_id": 430141,
                "entity_one_type": "EntityType.ARTIST",
                "entity_two_id": 3603,
                "entity_two_type": "EntityType.ARTIST",
                "random": None,
                "releases": None,
                "role": "Member Of",
            },
        ]
        actual = utils.normalize_dict_list(input_list)
        expected = """
        [
            {
                "entity_one_id": 430141,
                "entity_one_type": "EntityType.ARTIST",
                "entity_two_id": 307,
                "entity_two_type": "EntityType.ARTIST",
                "random": null,
                "releases": null,
                "role": "Member Of"
            },
            {
                "entity_one_id": 430141,
                "entity_one_type": "EntityType.ARTIST",
                "entity_two_id": 3603,
                "entity_two_type": "EntityType.ARTIST",
                "random": null,
                "releases": null,
                "role": "Member Of"
            }
        ]
        """
        self.assertEqual(utils.strip_input(expected), actual)

    def test_normalize_str_list(self):
        input_list = [
            "{\n    aaa\n    bbb\n    ccc\n}",
            "{\n    aaa\n    bbb\n    ccc\n}",
            "{\n    aaa\n    bbb\n    ccc\n}",
        ]

        actual = utils.normalize_str_list(input_list)
        expected = """
        [
            {
                aaa
                bbb
                ccc
            },
            {
                aaa
                bbb
                ccc
            },
            {
                aaa
                bbb
                ccc
            }
        ]
        """
        self.assertEqual(utils.strip_input(expected), actual)

    def test_strip_trailing_newline(self):
        input_str = "{\n    aaa\n    bbb\n    ccc\n}\n"

        actual = utils.strip_trailing_newline(input_str)
        expected = "{\n    aaa\n    bbb\n    ccc\n}"
        self.assertEqual(expected, actual)
