
from sqlalchemy.testing.provision import create_db
from sqlalchemy.testing.provision import drop_db
from sqlalchemy.testing.provision import configure_follower


@create_db.for_db("databend")
def _databend_create_db(cfg, eng, ident):
    with eng.begin() as conn:
        try:
            _databend_drop_db(cfg, conn, ident)
        except Exception:
            pass

    with eng.begin() as conn:
        conn.exec_driver_sql(
            "CREATE DATABASE IF NOT EXISTS %s " % ident
        )
        conn.exec_driver_sql(
            "CREATE DATABASE IF NOT EXISTS %s_test_schema" % ident
        )
        conn.exec_driver_sql(
            "CREATE DATABASE IF NOT EXISTS %s_test_schema_2" % ident
        )


@drop_db.for_db("databend")
def _databend_drop_db(cfg, eng, ident):
    with eng.begin() as conn:
        conn.exec_driver_sql("DROP DATABASE IF EXISTS %s_test_schema" % ident)
        conn.exec_driver_sql("DROP DATABASE IF EXISTS %s_test_schema_2" % ident)
        conn.exec_driver_sql("DROP DATABASE IF EXISTS %s" % ident)


@configure_follower.for_db("databend")
def _databend_configure_follower(config, ident):
    config.test_schema = "%s_test_schema" % ident
    config.test_schema_2 = "%s_test_schema_2" % ident
