# cursor description
from datetime import datetime, date
from pytest import mark, raises
from duckdb import InvalidInputException


class TestCursorDescription(object):
    @mark.parametrize(
        "query,column_name,string_type,real_type",
        [
            ["SELECT * FROM integers", "i", "NUMBER", int],
            ["SELECT * FROM timestamps", "t", "DATETIME", datetime],
            ["SELECT DATE '1992-09-20' AS date_col;", "date_col", "Date", date],
            ["SELECT '\\xAA'::BLOB AS blob_col;", "blob_col", "BINARY", bytes],
            ["SELECT {'x': 1, 'y': 2, 'z': 3} AS struct_col", "struct_col", "dict", dict],
            ["SELECT [1, 2, 3] AS list_col", "list_col", "list", list],
        ],
    )
    def test_description(
        self, query, column_name, string_type, real_type, duckdb_cursor
    ):
        duckdb_cursor.execute(query)
        assert duckdb_cursor.description == [
            (column_name, string_type, None, None, None, None, None)
        ]
        assert isinstance(duckdb_cursor.fetchone()[0], real_type)

    def test_none_description(self, duckdb_empty_cursor):
        assert duckdb_empty_cursor.description is None

    def test_rowcount(self, duckdb_cursor):
        ex = duckdb_cursor.execute
        assert ex('select 1').rowcount == -1  # not materialized
        duckdb_cursor.fetchall()
        assert duckdb_cursor.rowcount == 1

        assert ex('create table test (id int)').rowcount == -1  # does not update or return rows

        assert ex('insert into test values (1)').rowcount == 1
        assert ex('update test set id = 2').rowcount == 1
        assert ex('update test set id = 2 where id = 1').rowcount == 0  # no matched rows, so no updates
