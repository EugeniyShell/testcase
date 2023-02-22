import datetime
import unittest
import zipfile

import pandas

import main_parser
from base_class import BaseConnector, TableItem
from defaults import TEST_FILENAME, TEST_DATABASE_URI


class TestParser(unittest.TestCase):
    def test_get_file(self):
        self.assertRaises(
            FileNotFoundError,
            main_parser.get_file,
            '123.txt',
        )
        self.assertRaises(
            FileNotFoundError,
            main_parser.get_file,
            'qqq/123.txt',
        )
        self.assertRaises(
            FileNotFoundError,
            main_parser.get_file,
            'test_files/123.txt',
        )
        self.assertRaises(
            FileNotFoundError,
            main_parser.get_file,
            'test_files/tesfilenoexcel',
        )
        self.assertRaises(
            FileNotFoundError,
            main_parser.get_file,
            'test_files/tesfilenoexcel.txt',
        )
        checked = main_parser.get_file(TEST_FILENAME)
        self.assertEqual(checked, TEST_FILENAME)

    def test_read_file(self):
        self.assertRaises(
            zipfile.BadZipFile,
            main_parser.read_file,
            'test_files/tesfilenoexcel.xlsx',
        )
        self.assertRaises(
            pandas.errors.DataError,
            main_parser.read_file,
            'test_files/tesfilewrongsheets.xlsx',
        )
        self.assertRaises(
            pandas.errors.DataError,
            main_parser.read_file,
            'test_files/tesfilewrongheader.xlsx',
        )
        self.assertRaises(
            pandas.errors.DataError,
            main_parser.read_file,
            'test_files/tesfileempty.xlsx',
        )
        checked = main_parser.read_file(TEST_FILENAME)
        self.assertEqual(type(checked), pandas.core.frame.DataFrame)
        self.assertTrue(
            checked.equals(pandas.read_excel(
                pandas.ExcelFile(TEST_FILENAME, engine='openpyxl'),
                header=None, skiprows=3))
        )


class TestDataBase(unittest.TestCase):
    def setUp(self):
        self.BaseConnector = BaseConnector(dbUrl=TEST_DATABASE_URI)
        self.BaseConnector.current_session = self.BaseConnector.session()
        self.data = pandas.read_excel(pandas.ExcelFile(TEST_FILENAME,
                                                       engine='openpyxl'),
                                      header=None, skiprows=3)

    def tearDown(self):
        pass

    def test_class_init(self):
        self.assertEqual(self.BaseConnector.table, TableItem)
        self.assertEqual(list(self.BaseConnector.table.metadata.tables.keys()),
                         ['worktable'])

    def test_load_data(self):
        self.BaseConnector.load_data(self.data)
        self.assertEqual(len(self.BaseConnector.current_session.new),
                         self.data.shape[0] * 8)
        self.BaseConnector.current_session.rollback()

    def test_make_load_session(self):
        self.BaseConnector.make_load_session(self.data)
        self.BaseConnector.current_session = self.BaseConnector.session()
        t = self.BaseConnector.table
        q1 = self.BaseConnector.current_session.query(
            t.company, t.f_type, t.q_type, t.date, t.value
        ).first()
        q2 = self.BaseConnector.current_session.query(
            t.company, t.f_type, t.q_type, t.date, t.value
        ).filter(
            t.company == 'company2',
            t.f_type == 'forecast',
            t.q_type == 'Qoil'
        ).first()
        self.assertEqual(q1, ('company1', 'fact', 'Qliq',
                              datetime.date(2022, 12, 14), 10))
        self.assertEqual(q2, ('company2', 'forecast', 'Qoil',
                              datetime.date(2022, 12, 14), 20))

    def test_get_totals(self):
        self.BaseConnector.make_load_session(self.data)
        r = self.BaseConnector.get_totals()
        s = ('total', 41, 'fact', 'Qliq', datetime.date(2022, 12, 16))
        self.assertEqual(len(r), 24)
        self.assertEqual(r[14], s)


if __name__ == '__main__':
    unittest.main()
