from sqlalchemy.dialects import registry
from sqlalchemy.testing.plugin import pytestplugin
import pytest

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


from sqlalchemy import event, text
from sqlalchemy import Engine

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
