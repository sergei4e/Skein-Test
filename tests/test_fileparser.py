import unittest
import trafaret
from fileparser import FileParser


class TestFileParser(unittest.TestCase):

    def test_create_instance(self):
        try:
            p = FileParser()
            p.go()
        except Exception as e:
            self.assertEqual(type(e), TypeError)

    def test_validator(self):
        try:
            FileParser.validator({1: 2, 2: 3})
        except Exception as e:
            self.assertEqual(type(e), trafaret.dataerror.DataError)
