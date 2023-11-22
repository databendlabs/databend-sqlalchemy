from datetime import date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy.exc import OperationalError


class TestDatabendDialect:
    def test_set_params(
            self, username: str, password: str, database_name: str, host_port_name: str
    ):
        engine = create_engine(
            f"databend://{username}:{password}@{host_port_name}/{database_name}?secure=false"
        )
        connection = engine.connect()
        result = connection.execute(text("SELECT 1"))
        assert len(result.fetchall()) == 1
        engine.dispose()

    def test_data_write(self, connection: Connection, fact_table_name: str):
        connection.execute(
            text(f"INSERT INTO {fact_table_name}(idx, dummy) VALUES (1, 'some_text')")
        )
        result = connection.execute(
            text(f"SELECT * FROM {fact_table_name} WHERE idx=1")
        )
        assert result.fetchall() == [(1, "some_text")]
        result = connection.execute(text(f"SELECT * FROM {fact_table_name}"))
        assert len(result.fetchall()) == 1

    def test_databend_types(self, connection: Connection):
        result = connection.execute(text("SELECT to_date('1896-01-01')"))
        print(str(date(1896, 1, 1)))
        assert result.fetchall() == [("1896-01-01",)]

    def test_has_table(
            self, engine: Engine, connection: Connection, fact_table_name: str
    ):
        results = engine.dialect.has_table(connection, fact_table_name)
        assert results == 1

    def test_get_columns(
            self, engine: Engine, connection: Connection, fact_table_name: str
    ):
        results = engine.dialect.get_columns(connection, fact_table_name)
        assert len(results) > 0
        row = results[0]
        assert isinstance(row, dict)
        row_keys = list(row.keys())
        row_values = list(row.values())
        assert row_keys[0] == "name"
        assert row_keys[1] == "type"
        assert row_keys[2] == "nullable"
        assert row_keys[3] == "default"
        print(row_values)
