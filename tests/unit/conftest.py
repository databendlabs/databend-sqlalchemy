from unittest import mock
from databend_sqlalchemy import databend_dialect
from pytest import fixture


class MockDBApi:
    class DatabaseError:
        pass

    class Error:
        pass

    class IntegrityError:
        pass

    class NotSupportedError:
        pass

    class OperationalError:
        pass

    class ProgrammingError:
        pass

    paramstyle = ""

    def execute():
        pass

    def executemany():
        pass

    def connect():
        pass


class MockCursor:
    def execute():
        pass

    def executemany():
        pass

    def fetchall():
        pass

    def close():
        pass


@fixture
def dialect() -> databend_dialect.DatabendDialect:
    return databend_dialect.DatabendDialect()


@fixture
def connection() -> mock.Mock(spec=MockDBApi):
    return mock.Mock(spec=MockDBApi)


@fixture
def cursor() -> mock.Mock(spec=MockCursor):
    return mock.Mock(spec=MockCursor)
