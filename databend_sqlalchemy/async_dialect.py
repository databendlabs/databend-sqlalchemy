#!/usr/bin/env python
"""
Databend Asyncio Support
------------------------

The ``databend+async`` dialect runs on the asyncio flavour of ``databend-driver``
through :mod:`databend_sqlalchemy.async_connector`, and needs SQLAlchemy 2.0.28 or
later, the first release to report the row counts Databend returns for inserts::

    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine("databend+async://user:password@host:8000/database")

``create_async_engine`` given a plain ``databend://`` URL selects this dialect as well.

Results are buffered in full when a statement executes, unless streamed with
``AsyncConnection.stream()`` or the ``stream_results`` execution option, which fetch
rows from Databend as they are consumed.
"""
import re
from collections import deque
from collections.abc import Sequence
from types import ModuleType
from typing import Any

import sqlalchemy
from sqlalchemy import pool
from sqlalchemy.engine.url import URL
from sqlalchemy.util.concurrency import await_only

from databend_sqlalchemy import async_connector
from databend_sqlalchemy.connector import Parameters
from databend_sqlalchemy.databend_dialect import (
    DatabendDialect,
    DatabendExecutionContext,
)

ASYNCIO_REQUIREMENT = (
    "databend+async needs SQLAlchemy 2.0.28 or later: "
    'pip install "databend-sqlalchemy[asyncio]"'
)

try:
    from sqlalchemy.connectors.asyncio import (
        AsyncAdapt_dbapi_connection,
        AsyncAdapt_dbapi_cursor,
        AsyncAdapt_dbapi_ss_cursor,
    )
except ImportError as missing_adapter:
    raise ImportError(ASYNCIO_REQUIREMENT) from missing_adapter


def installed_sqlalchemy_release() -> tuple[int, ...]:
    """Major, minor and patch number of the installed SQLAlchemy, ignoring any
    pre-release tag."""
    release_numbers = re.findall(r"\d+", sqlalchemy.__version__)[:3]
    return tuple(int(number) for number in release_numbers)


if installed_sqlalchemy_release() < (2, 0, 28):
    raise ImportError(ASYNCIO_REQUIREMENT)


class AsyncAdaptedDatabendCursor(AsyncAdapt_dbapi_cursor):
    """Buffers the rows an ``executemany`` leaves behind as well as those of
    ``execute``, as :meth:`DatabendExecutionContext.post_exec` reads the inserted row
    count back after both."""

    __slots__ = ()

    async def _executemany_async(
        self, operation: str, seq_of_parameters: Sequence[Parameters]
    ) -> None:
        async with self._adapt_connection._execute_mutex:
            await self._cursor.executemany(operation, seq_of_parameters)
            if self._cursor.description:
                self._rows = deque(await self._cursor.fetchall())


class AsyncAdaptedDatabendConnection(AsyncAdapt_dbapi_connection):
    __slots__ = ()

    _cursor_cls = AsyncAdaptedDatabendCursor


class AsyncAdaptedDatabendDBAPI:
    """Presents :mod:`databend_sqlalchemy.async_connector` to SQLAlchemy as a
    synchronous DBAPI module, each await being bridged through the calling greenlet."""

    def __init__(self, connector: ModuleType) -> None:
        self.connector = connector
        self.apilevel = connector.apilevel
        self.threadsafety = connector.threadsafety
        self.paramstyle = connector.paramstyle
        self.Binary = connector.Binary
        self.Error = connector.Error
        self.NotSupportedError = connector.NotSupportedError

    def connect(self, *args: Any, **kwargs: Any) -> AsyncAdaptedDatabendConnection:
        creator_fn = kwargs.pop("async_creator_fn", self.connector.connect)
        return AsyncAdaptedDatabendConnection(
            self, await_only(creator_fn(*args, **kwargs))
        )


class AsyncDatabendExecutionContext(DatabendExecutionContext):
    def create_server_side_cursor(self) -> AsyncAdapt_dbapi_ss_cursor:
        return self._dbapi_connection.cursor(server_side=True)


class AsyncDatabendDialect(DatabendDialect):
    driver = "async"
    is_async = True
    supports_statement_cache = False

    execution_ctx_cls = AsyncDatabendExecutionContext

    @classmethod
    def import_dbapi(cls) -> AsyncAdaptedDatabendDBAPI:
        return AsyncAdaptedDatabendDBAPI(async_connector)

    @classmethod
    def get_pool_class(cls, url: URL) -> type[pool.Pool]:
        return pool.AsyncAdaptedQueuePool

    def get_driver_connection(
        self, connection: AsyncAdaptedDatabendConnection
    ) -> async_connector.AsyncConnection:
        return connection._connection


dialect = AsyncDatabendDialect
