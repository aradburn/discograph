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
