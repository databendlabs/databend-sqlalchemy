#!/usr/bin/env python

from sqlalchemy import Column, Integer, String, Table, func, insert, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.testing import (
    async_test,
    config,
    eq_,
    expect_raises,
    fixtures,
    is_not_none,
    is_true,
)

from databend_sqlalchemy import async_connector
from databend_sqlalchemy.async_dialect import AsyncDatabendDialect


class AsyncDialectSelectionTest(fixtures.TestBase):
    __requires__ = ("asyncio",)

    def test_create_async_engine_accepts_plain_url(self):
        engine = create_async_engine(config.db.url.set(drivername="databend"))
        is_true(isinstance(engine.dialect, AsyncDatabendDialect))


class AsyncEngineTest(fixtures.TablesTest):
    """Exercises the asyncio API itself, so it runs under ``--db async``, where the
    harness disposes of async engines inside a greenlet."""

    __backend__ = True
    __requires__ = ("async_dialect",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "async_users",
            metadata,
            Column("id", Integer),
            Column("name", String(50)),
        )

    @async_test
    async def test_buffered_result_outlives_connection(self, async_testing_engine):
        users = self.tables.async_users
        engine = async_testing_engine()
        async with engine.begin() as conn:
            await conn.execute(
                insert(users), [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
            )
            result = await conn.execute(select(users).order_by(users.c.id))
        eq_(result.all(), [(1, "a"), (2, "b")])

    @async_test
    async def test_insert_rowcount(self, async_testing_engine):
        users = self.tables.async_users
        engine = async_testing_engine()
        async with engine.begin() as conn:
            single = await conn.execute(insert(users).values(id=1, name="a"))
            many = await conn.execute(
                insert(users), [{"id": i, "name": f"n{i}"} for i in range(2, 5)]
            )
        eq_(single.rowcount, 1)
        eq_(many.rowcount, 3)

    @async_test
    async def test_stream_fetches_rows_as_consumed(self, async_testing_engine):
        engine = async_testing_engine()
        async with engine.connect() as conn:
            result = await conn.stream(text("SELECT number FROM numbers(2500)"))
            driver_cursor = result._real_result.cursor._cursor
            partitions = result.partitions(1000)
            partition_sizes = [len(await anext(partitions))]
            is_not_none(driver_cursor._rows)
            partition_sizes += [len(p) async for p in partitions]
        eq_(partition_sizes, [1000, 1000, 500])

    @async_test
    async def test_session_state_shared_across_statements(self, async_testing_engine):
        engine = async_testing_engine()
        async with engine.connect() as conn:
            await conn.execute(
                text("CREATE OR REPLACE TEMPORARY TABLE async_tmp (a INT)")
            )
            await conn.execute(text("INSERT INTO async_tmp VALUES (1), (2)"))
            eq_(await conn.scalar(text("SELECT count(*) FROM async_tmp")), 2)

    @async_test
    async def test_driver_errors_raise_dbapi_error(self, async_testing_engine):
        engine = async_testing_engine()
        async with engine.connect() as conn:
            with expect_raises(DBAPIError):
                await conn.execute(text("SELECT * FROM async_no_such_table"))
            eq_(await conn.scalar(text("SELECT 1")), 1)

    @async_test
    async def test_async_creator(self, async_testing_engine):
        dialect = async_testing_engine().dialect
        _, connect_args = dialect.create_connect_args(config.db.url)
        engine = create_async_engine(
            "databend+async://",
            async_creator=lambda: async_connector.connect(**connect_args),
        )
        try:
            async with engine.connect() as conn:
                eq_(await conn.scalar(text("SELECT 42")), 42)
        finally:
            await engine.dispose()

    @async_test
    async def test_orm_session_round_trip(self, async_testing_engine):
        class Base(DeclarativeBase):
            pass

        class Note(Base):
            __tablename__ = "async_notes"
            id: Mapped[int] = mapped_column(primary_key=True)
            body: Mapped[str] = mapped_column(String(50))

        engine = async_testing_engine()
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        try:
            async with AsyncSession(engine) as session:
                session.add_all([Note(id=1, body="first"), Note(id=2, body="second")])
                await session.commit()
            async with AsyncSession(engine) as session:
                notes = (await session.scalars(select(Note).order_by(Note.id))).all()
                eq_([(n.id, n.body) for n in notes], [(1, "first"), (2, "second")])
                eq_(await session.scalar(select(func.count()).select_from(Note)), 2)
        finally:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.drop_all)
