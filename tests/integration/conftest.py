from logging import getLogger
from os import environ

from pytest import fixture
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Connection, Engine

LOGGER = getLogger(__name__)

DATABASE_NAME = "default"
USERNAME = "root"
PASSWORD = "root"
HOST_PORT = "localhost:8002"


def must_env(var_name: str) -> str:
    assert var_name in environ, f"Expected {var_name} to be provided in environment"
    LOGGER.info(f"{var_name}: {environ[var_name]}")
    return environ[var_name]


@fixture(scope="session")
def host_port_name() -> str:
    return HOST_PORT


@fixture(scope="session")
def database_name() -> str:
    return DATABASE_NAME


@fixture(scope="session")
def username() -> str:
    return USERNAME


@fixture(scope="session")
def password() -> str:
    return PASSWORD


@fixture(scope="session")
def engine(
    username: str, password: str, host_port_name: str, database_name: str
) -> Engine:
    return create_engine(
        f"databend://{username}:{password}@{host_port_name}/{database_name}?secure=false"
    )


@fixture(scope="session")
def connection(engine: Engine) -> Connection:
    with engine.connect() as c:
        yield c


@fixture(scope="class")
def fact_table_name() -> str:
    return "test_alchemy"


@fixture(scope="class")
def dimension_table_name() -> str:
    return "test_alchemy_dimension"


@fixture(scope="class", autouse=True)
def setup_test_tables(
    connection: Connection,
    engine: Engine,
    fact_table_name: str,
    dimension_table_name: str,
):
    connection.execute(
        text(
            f"""
        CREATE TABLE IF NOT EXISTS {fact_table_name}
        (
            idx INT,
            dummy VARCHAR
        );
        """
        )
    )
    connection.execute(
        text(
            f"""
        CREATE TABLE IF NOT EXISTS {dimension_table_name}
        (
            idx INT,
            dummy VARCHAR
        );
        """
        )
    )
    assert engine.dialect.has_table(connection, fact_table_name)
    assert engine.dialect.has_table(connection, dimension_table_name)
    yield
    # Teardown
    connection.execute(text(f"DROP TABLE IF EXISTS {fact_table_name};"))
    connection.execute(text(f"DROP TABLE IF EXISTS {dimension_table_name};"))
    assert not engine.dialect.has_table(connection, fact_table_name)
    assert not engine.dialect.has_table(connection, dimension_table_name)
