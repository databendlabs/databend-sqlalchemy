#!/usr/bin/env python

import datetime
import decimal

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    Float,
    Integer,
    Numeric,
    String,
    Table,
    inspect,
    insert,
    select,
    text,
)
from sqlalchemy.testing import eq_, fixtures, is_false, is_true


class BasicUsageTest(fixtures.TablesTest):
    """Covers what SQL tools such as Superset rely on. Unlike the compliance suite, this
    runs on every supported SQLAlchemy release, SQLAlchemy 1.4 included."""

    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "basic_usage",
            metadata,
            Column("id", Integer),
            Column("name", String(50)),
            Column("score", Float),
            Column("active", Boolean),
            Column("joined", Date),
            Column("balance", Numeric(10, 2)),
        )

    def test_inspector_describes_table(self, connection):
        inspector = inspect(connection)
        is_true("basic_usage" in inspector.get_table_names())
        is_true(inspector.has_table("basic_usage"))
        is_false(inspector.has_table("basic_usage_missing"))
        eq_(
            [column["name"] for column in inspector.get_columns("basic_usage")],
            ["id", "name", "score", "active", "joined", "balance"],
        )

    def test_inspector_describes_view(self, connection):
        connection.exec_driver_sql(
            "CREATE OR REPLACE VIEW basic_usage_view AS SELECT id, name FROM basic_usage"
        )
        try:
            inspector = inspect(connection)
            is_true("basic_usage_view" in inspector.get_view_names())
            eq_(
                [column["name"] for column in inspector.get_columns("basic_usage_view")],
                ["id", "name"],
            )
        finally:
            connection.exec_driver_sql("DROP VIEW IF EXISTS basic_usage_view")

    def test_executemany_then_limit_offset(self, connection):
        table = self.tables.basic_usage
        connection.execute(
            insert(table), [{"id": i, "name": f"n{i}"} for i in range(1, 6)]
        )
        rows = connection.execute(
            select(table.c.id).order_by(table.c.id).limit(2).offset(1)
        ).all()
        eq_(rows, [(2,), (3,)])

    def test_text_with_percent_and_bound_parameter(self, connection):
        eq_(connection.scalar(text("SELECT 'a%b'")), "a%b")
        eq_(
            connection.scalar(text("SELECT CONCAT(:value, '%')"), {"value": "50"}),
            "50%",
        )

    def test_types_round_trip(self, connection):
        table = self.tables.basic_usage
        row = {
            "id": 1,
            "name": "it's",
            "score": 1.5,
            "active": True,
            "joined": datetime.date(2026, 10, 23),
            "balance": decimal.Decimal("12.34"),
        }
        connection.execute(insert(table).values(**row))
        eq_(connection.execute(select(table)).one()._asdict(), row)
