from sqlalchemy.dialects import registry
import pytest

registry.register("databend.databend", "databend_sqlalchemy.databend_dialect", "DatabendDialect")
registry.register("databend", "databend_sqlalchemy.databend_dialect", "DatabendDialect")

pytest.register_assert_rewrite("sa.testing.assertions")

from sqlalchemy.testing.plugin.pytestplugin import *
