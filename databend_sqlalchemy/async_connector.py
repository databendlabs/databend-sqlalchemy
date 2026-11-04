#!/usr/bin/env python
"""
Asyncio counterpart of :mod:`databend_sqlalchemy.connector`, built on the driver's
``AsyncDatabendClient``.

The driver offers no async cursor, so :class:`AsyncCursor` supplies the :pep:`249`
cursor surface as coroutines, rendering parameters exactly as the blocking cursor does.
Unlike the blocking connector, which opens a driver connection per cursor, every cursor
of an :class:`AsyncConnection` shares one driver connection, and so one Databend
session.
"""
import asyncio
from collections import deque
from collections.abc import Sequence
from itertools import islice
from types import TracebackType

from databend_driver import (
    AsyncDatabendClient,
    AsyncDatabendConnection,
    Row,
    RowIterator,
)

from databend_sqlalchemy.connector import (  # noqa: F401 - PEP 249 module globals
    Binary,
    OperationRenderer,
    Parameters,
    apilevel,
    paramstyle,
    threadsafety,
)
from databend_sqlalchemy.errors import Error, NotSupportedError

ColumnDescription = tuple[str, str, None, None, None, None, None]


async def connect(
    dsn: str = "databend://root:@localhost:8000/?sslmode=disable",
) -> "AsyncConnection":
    try:
        driver_connection = await AsyncDatabendClient(dsn).get_conn()
    except Exception as e:
        raise Error(str(e)) from e
    return AsyncConnection(driver_connection)


class AsyncConnection:
    def __init__(self, driver_connection: AsyncDatabendConnection) -> None:
        self.driver_connection = driver_connection

    async def close(self) -> None:
        """Close the Databend session, where the driver is new enough to offer that."""
        close_session = getattr(self.driver_connection, "close", None)
        if close_session is not None:
            await close_session()

    async def commit(self) -> None:
        pass

    def cursor(self) -> "AsyncCursor":
        return AsyncCursor(self.driver_connection)

    async def rollback(self) -> None:
        raise NotSupportedError("Transactions are not supported")  # pragma: no cover


class AsyncCursor(OperationRenderer):
    """Asyncio counterpart of :class:`databend_sqlalchemy.connector.Cursor`.

    As with the blocking driver cursor, every statement runs through ``query_iter`` and
    its first row is read straight away, so a statement without a result set has
    finished by the time :meth:`execute` returns.
    """

    arraysize = 1

    def __init__(self, driver_connection: AsyncDatabendConnection) -> None:
        self.driver_connection = driver_connection
        self.description: list[ColumnDescription] | None = None
        self._rows: RowIterator | None = None
        self._read_ahead: deque[Row] = deque()

    @property
    def rowcount(self) -> int:
        """By default, return -1 to indicate that this is not supported."""
        return -1

    async def __aenter__(self) -> "AsyncCursor":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        """Discard any unread rows, leaving the shared session open."""
        self._discard_results()

    async def execute(
        self, operation: str, parameters: Parameters | None = None
    ) -> None:
        """Prepare and execute a database operation (query or command).

        Blank operations, such as the empty DDL compiled for unsupported index
        statements, are skipped just as the blocking cursor skips them.
        """
        if not operation:
            return
        try:
            await self._run_query(self.render_operation(operation, parameters))
        except Exception as e:
            raise Error(str(e)) from e

    async def executemany(
        self, operation: str, seq_of_parameters: Sequence[Parameters]
    ) -> None:
        """Run ``operation`` once per parameter set, sending ``INSERT ... VALUES`` as a
        single multi-row statement. Only the final result set is retained."""
        try:
            query = self.render_multi_row_insert(operation, seq_of_parameters)
            if query is not None:
                return await self._run_query(query)
        except Exception as e:
            raise Error(str(e)) from e
        for parameters in seq_of_parameters:
            await self.execute(operation, parameters)

    async def fetchone(self) -> tuple | None:
        row = await self._next_row()
        return None if row is None else row.values()

    async def fetchmany(self, size: int | None = None) -> list[tuple]:
        return await self._fetch_rows(self.arraysize if size is None else size)

    async def fetchall(self) -> list[tuple]:
        return await self._fetch_rows()

    def __aiter__(self) -> "AsyncCursor":
        return self

    async def __anext__(self) -> tuple:
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    def _discard_results(self) -> None:
        self.description = None
        self._rows = None
        self._read_ahead.clear()

    async def _run_query(self, query: str) -> None:
        self._discard_results()
        rows = await self.driver_connection.query_iter(query)
        self.description = [
            (field.name, field.data_type, None, None, None, None, None)
            for field in rows.schema().fields()
        ]
        self._rows = rows
        if (first_row := await anext(rows, None)) is not None:
            self._read_ahead.append(first_row)

    async def _next_row(self) -> Row | None:
        if self._read_ahead:
            return self._read_ahead.popleft()
        if self._rows is None:
            return None
        try:
            return await anext(self._rows, None)
        except Exception as e:
            raise Error(str(e)) from e

    async def _fetch_rows(self, limit: int | None = None) -> list[tuple]:
        """Return up to ``limit`` rows, or every remaining row when ``limit`` is
        ``None``.

        Awaiting the driver's iterator costs a trip through its runtime for every row,
        which is around a hundred times slower than iterating it directly. Rows beyond
        the read-ahead are therefore drained with the iterator's blocking ``__next__``
        in a worker thread, one trip per batch. The iterator is released once
        exhausted.
        """
        rows = []
        while self._read_ahead and (limit is None or len(rows) < limit):
            rows.append(self._read_ahead.popleft().values())
        wanted = None if limit is None else limit - len(rows)
        if self._rows is None or wanted == 0:
            return rows
        batch = await asyncio.to_thread(self._drain_rows, self._rows, wanted)
        if wanted is None or len(batch) < wanted:
            self._rows = None
        return rows + batch

    @staticmethod
    def _drain_rows(rows: RowIterator, limit: int | None) -> list[tuple]:
        try:
            return [row.values() for row in islice(rows, limit)]
        except Exception as e:
            raise Error(str(e)) from e
