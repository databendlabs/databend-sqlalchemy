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
        from sqlalchemy.dialects import registry

        registry.register('databend', 'databend_sqlalchemy.databend_dialect.dialect')
        engine = create_engine(
            f"databend://{username}:{password}@{host_port_name}/{database_name}"
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
        # Update not supported
        with pytest.raises(OperationalError):
            connection.execute(
                text(
                    f"UPDATE {fact_table_name} SET dummy='some_other_text' WHERE idx=1"
                )
            )
