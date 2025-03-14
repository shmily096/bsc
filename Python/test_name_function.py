import unittest
from name_function import get_formatted_name
class NamesTestCase(unittest.TestCase):
    """测试name_function.py"""
    def test_first_last_name(self):
        formatted_name=get_formatted_name('amy','lie')
        self.assertEqual(formatted_name,'Amy Lie')
print(NamesTestCase.__doc__)
unittest.main()