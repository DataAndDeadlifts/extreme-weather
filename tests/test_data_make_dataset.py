import unittest
from src.data import make_dataset


class TestDataDir(unittest.TestCase):
    def test_data_dir_ref_good(self):
        self.assertTrue(make_dataset.DATA_DIR.exists())


if __name__ == '__main__':
    unittest.main()
