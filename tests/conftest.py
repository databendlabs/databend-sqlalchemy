from sqlalchemy.dialects import registry
from sqlalchemy import event, Engine, text
import pytest

registry.register("databend.databend", "databend_sqlalchemy.databend_dialect", "DatabendDialect")
registry.register("databend", "databend_sqlalchemy.databend_dialect", "DatabendDialect")

pytest.register_assert_rewrite("sa.testing.assertions")

from sqlalchemy.testing.plugin.pytestplugin import *


@event.listens_for(Engine, "connect")
def receive_engine_connect(conn, r):
    cur = conn.cursor()
    cur.execute('SET global format_null_as_str = 0')
    cur.close()
