import unittest
from unittest.mock import patch

import mistral_db


class FakeCursorFieldLen:
    def __init__(self):
        self.execute_calls = 0
        self.last_params = None

    def execute(self, sql, params):
        self.execute_calls += 1
        self.last_params = params

    def fetchone(self):
        return (10,)


class FakeCursorItems:
    def __init__(self):
        self.description = []
        self.executed_sql = ""
        self.executed_params = []

    def execute(self, sql, params):
        self.executed_sql = sql
        self.executed_params.append(params)
        self.description = [
            ("ITEM_ID", None, None, None, None, None, None),
            ("ITEM_CODE", None, None, None, None, None, None),
            ("ITEM_NAME", None, None, None, None, None, None),
            ("ITEM_BARCODE", None, None, None, None, None, None),
        ]

    def fetchall(self):
        return [(1, "ABC", "Test Name", "123")]


class MistralDBLookupTests(unittest.TestCase):
    def setUp(self):
        mistral_db._FIELD_LENGTH_CACHE.clear()

    def test_get_field_max_len_uses_cache(self):
        cursor = FakeCursorFieldLen()
        with patch.object(mistral_db, "_require_cursor", return_value=cursor):
            length_first = mistral_db.get_field_max_len(cursor, "MATERIAL", "NAME")
            length_second = mistral_db.get_field_max_len(cursor, "material", "name")
        self.assertEqual(length_first, 10)
        self.assertEqual(length_second, 10)
        self.assertEqual(cursor.execute_calls, 1)
        self.assertEqual(cursor.last_params, ("MATERIAL", "NAME"))

    def test_get_items_by_name_truncates_parameter(self):
        cursor = FakeCursorItems()
        schema = {
            "materials_table": "MATERIAL",
            "materials_name": "MATERIAL",
            "materials_code": "MATERIALCODE",
            "materials_id": "ID",
            "materials_uom": None,
            "materials_price": None,
            "materials_vat": None,
            "barcode_table": "BARCODE",
            "barcode_col": "CODE",
            "barcode_mat_fk": "STORAGEMATERIALCODE",
        }
        with patch.object(mistral_db, "detect_catalog_schema", return_value=schema):
            with patch.object(mistral_db, "get_field_max_len", return_value=5):
                with patch.object(mistral_db, "_require_cursor", return_value=cursor):
                    items = mistral_db.get_items_by_name(cursor, "   Дълго   име   ", limit=2)
        self.assertTrue(items)
        self.assertIn("CONTAINING ?", cursor.executed_sql)
        self.assertEqual(cursor.executed_params[0][0], "Дълго")
        self.assertEqual(items[0]["code"], "ABC")
        self.assertEqual(items[0]["barcode"], "123")


if __name__ == "__main__":
    unittest.main()
