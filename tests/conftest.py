from packaging import version
from sqlalchemy.dialects import registry
from sqlalchemy.testing.plugin import pytestplugin
import pytest
import sqlalchemy

registry.register("databend.databend", "databend_sqlalchemy.databend_dialect", "DatabendDialect")
registry.register("databend", "databend_sqlalchemy.databend_dialect", "DatabendDialect")
registry.register("databend.async", "databend_sqlalchemy.async_dialect", "AsyncDatabendDialect")

pytest.register_assert_rewrite("sa.testing.assertions")

from sqlalchemy.testing.plugin.pytestplugin import *


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(session, config, items):
    """Expand SQLAlchemy's per-backend test classes before pytest-xdist reads the
    ``xdist_group`` markers for ``--dist loadgroup``; xdist would otherwise tag the
    items SQLAlchemy is about to replace."""
    pytestplugin.pytest_collection_modifyitems(session, config, items)


def modules_beyond_installed_sqlalchemy():
    """The compliance suite relies on test requirements SQLAlchemy added in 2.0.42 and the
    asyncio dialect needs 2.0.28, so neither module is collected on older releases, which
    still run the remaining tests, SQLAlchemy 1.4 included."""
    installed = version.parse(sqlalchemy.__version__)
    minimums = {"test_sqlalchemy.py": "2.0.42", "test_async.py": "2.0.28"}
    return [
        module
        for module, minimum in minimums.items()
        if installed < version.parse(minimum)
    ]


collect_ignore = modules_beyond_installed_sqlalchemy()

from sqlalchemy import event, text
from sqlalchemy.engine import Engine

@event.listens_for(Engine, "connect")
def receive_engine_connect(conn, r):
    cur = conn.cursor()
    cur.execute('SET global format_null_as_str = 0')
    cur.execute('SET global enable_geo_create_table = 1')
    try:
        cur.execute("SET global sequence_step_size = 1")
    except:
        pass # don't fail when this setting doesn't exist
    cur.close()
