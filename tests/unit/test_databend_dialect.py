import os
from unittest import mock

import sqlalchemy
from conftest import MockCursor, MockDBApi
from pytest import mark
from sqlalchemy.engine import url
from sqlalchemy.sql import text

import databend_sqlalchemy  # SQLAlchemy package
from databend_sqlalchemy.databend_dialect import (
    DatabendCompiler,
    DatabendDialect,
    DatabendIdentifierPreparer,
    DatabendTypeCompiler,
)
from databend_sqlalchemy.databend_dialect import dialect as dialect_definition


class TestDatabendDialect:
    def test_create_dialect(self, dialect: DatabendDialect):
        assert issubclass(dialect_definition, DatabendDialect)
        assert isinstance(DatabendDialect.dbapi(), type(databend_sqlalchemy))
        assert dialect.name == "databend"
        assert dialect.driver == "databend"
        assert issubclass(dialect.preparer, DatabendIdentifierPreparer)
        assert issubclass(dialect.statement_compiler, DatabendCompiler)
        # SQLAlchemy's DefaultDialect creates an instance of
        # type_compiler behind the scenes
        assert isinstance(dialect.type_compiler, DatabendTypeCompiler)
        assert dialect.context == {}

    def test_create_connect_args_service_account(self, dialect: DatabendDialect):
        u = url.make_url(
            "databend://user:pass@localhost:8000/testdb?secure=false"
        )

        result_list, result_dict = dialect.create_connect_args(u)
        assert result_dict["db_url"] == "databend://localhost:8000/"
        assert result_dict["database"] == "testdb"
        assert result_dict["username"] == "user"

    def test_do_execute(
            self, dialect: DatabendDialect, cursor: mock.Mock(spec=MockCursor)
    ):
        dialect.do_execute(cursor, "SELECT *", None)
        cursor.execute.assert_called_once_with("SELECT *", None)
        cursor.execute.reset_mock()
        dialect.do_execute(cursor, "SELECT *", (1, 22), None)

    def test_table_names(
            self, dialect: DatabendDialect, connection: mock.Mock(spec=MockDBApi)
    ):
        def row_with_table_name(name):
            return mock.Mock(table_name=name)

        connection.execute.return_value = [
            row_with_table_name("table1"),
            row_with_table_name("table2"),
        ]

        result = dialect.get_table_names(connection)
        assert result == ["table1", "table2"]
        connection.execute.assert_called_once()
        assert str(connection.execute.call_args[0][0].compile()) == str(
            text("select table_name from information_schema.tables").compile()
        )
        connection.execute.reset_mock()
        result = dialect.get_table_names(connection, schema="schema")
        assert result == ["table1", "table2"]
        connection.execute.assert_called_once()
        assert str(connection.execute.call_args[0][0].compile()) == str(
            text(
                "select table_name from information_schema.tables"
                " where table_schema = 'schema'"
            ).compile()
        )

    def test_view_names(
            self, dialect: DatabendDialect, connection: mock.Mock(spec=MockDBApi)
    ):
        connection.execute.return_value = []
        assert dialect.get_view_names(connection) == []

    def test_columns(
            self, dialect: DatabendDialect, connection: mock.Mock(spec=MockDBApi)
    ):
        def multi_column_row(columns):
            def getitem(self, idx):
                for i, result in enumerate(columns):
                    if idx == i:
                        return result

            return mock.Mock(__getitem__=getitem)

        connection.execute.return_value = [
            multi_column_row(["name1", "INT", "YES"]),
            multi_column_row(["name2", "date", "NO"]),
        ]

        expected_query = """
            select column_name,
                   data_type,
                   is_nullable
              from information_schema.columns
             where table_name = 'table'
        """

        expected_query_schema = expected_query + " and table_schema = 'schema'"

        for call, expected_query in (
                (lambda: dialect.get_columns(connection, "table"), expected_query),
                (
                        lambda: dialect.get_columns(connection, "table", "schema"),
                        expected_query_schema,
                ),
        ):
            assert call() == [
                {
                    "name": "name1",
                    "type": sqlalchemy.types.INTEGER,
                    "nullable": True,
                    "default": None,
                },
                {
                    "name": "name2",
                    "type": sqlalchemy.types.DATE,
                    "nullable": False,
                    "default": None,
                },
            ]
            connection.execute.assert_called_once()
            assert str(connection.execute.call_args[0][0].compile()) == str(
                text(expected_query).compile()
            )
            connection.execute.reset_mock()


def test_get_is_nullable():
    assert databend_sqlalchemy.databend_dialect.get_is_nullable("YES")
    assert not databend_sqlalchemy.databend_dialect.get_is_nullable("NO")


def test_types():
    assert databend_sqlalchemy.databend_dialect.CHAR is sqlalchemy.sql.sqltypes.CHAR
    assert databend_sqlalchemy.databend_dialect.DATE is sqlalchemy.sql.sqltypes.DATE
    assert databend_sqlalchemy.databend_dialect.DATETIME is sqlalchemy.sql.sqltypes.DATETIME
    assert databend_sqlalchemy.databend_dialect.INTEGER is sqlalchemy.sql.sqltypes.INTEGER
    assert databend_sqlalchemy.databend_dialect.BIGINT is sqlalchemy.sql.sqltypes.BIGINT
    assert databend_sqlalchemy.databend_dialect.TIMESTAMP is sqlalchemy.sql.sqltypes.TIMESTAMP
    assert databend_sqlalchemy.databend_dialect.VARCHAR is sqlalchemy.sql.sqltypes.VARCHAR
    assert databend_sqlalchemy.databend_dialect.BOOLEAN is sqlalchemy.sql.sqltypes.BOOLEAN
    assert databend_sqlalchemy.databend_dialect.FLOAT is sqlalchemy.sql.sqltypes.FLOAT
    assert issubclass(databend_sqlalchemy.databend_dialect.ARRAY, sqlalchemy.types.TypeEngine)
