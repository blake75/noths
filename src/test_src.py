import unittest, os, inspect
from src import *

this_file_path=os.path.abspath(inspect.getsourcefile(lambda:0)) #location of this file
csv_path=os.path.join(this_file_path, "..", "..", "nypd-arrest-data-2018-1.csv") 
csv_path=os.path.normpath(csv_path)
data=load_nypd_data(csv_path)

class TestSrc(unittest.TestCase):

    def test_nypd_schema(self):
        key_count=len(nypd_schema.keys())
        self.assertEqual(key_count, 18)

    def test_load_nypd_data(self):
        records=len(data)
        self.assertEqual(records, 131044)

    def test_groupby_count(self):
        #just test a sample here and see if it runs and returns a dictionary
        object_type=type(groupby_count(data, ["OFNS_DESC"]))
        self.assertEqual(object_type, dict)

    def test_requirement_1(self):
        offenses=requirement_1(data)
        records=len(offenses)
        self.assertEqual(records, 10)

    def test_requirement_2(self):
        results=requirement_2(data)
        self.assertIsInstance(results, str)

    def test_requirement_3(self):
        csv_path=os.path.join("test_files", "test.csv")
        fullfilepath=requirement_3(data, csv_path)
        self.assertTrue(fullfilepath)

    def test_requirement_4(self):
        test_path=os.path.join("test_files", "test.db")
        print(os.path.abspath(test_path))
        log=requirement_4(data, test_path)
        self.assertTrue(log)

if __name__ == '__main__':
    unittest.main()