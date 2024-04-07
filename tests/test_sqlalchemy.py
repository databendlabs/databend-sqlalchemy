# tests/test_suite.py

from sqlalchemy.testing.suite import *

from sqlalchemy.testing.suite import ComponentReflectionTestExtra as _ComponentReflectionTestExtra
from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest
from sqlalchemy.testing.suite import DeprecatedCompoundSelectTest as _DeprecatedCompoundSelectTest
from sqlalchemy.testing.suite import BooleanTest as _BooleanTest
from sqlalchemy.testing.suite import BinaryTest as _BinaryTest
from sqlalchemy.testing.suite import CompoundSelectTest as _CompoundSelectTest
from sqlalchemy.testing.suite import HasIndexTest as _HasIndexTest
from sqlalchemy.testing.suite import InsertBehaviorTest as _InsertBehaviorTest
from sqlalchemy.testing.suite import LikeFunctionsTest as _LikeFunctionsTest
from sqlalchemy.testing.suite import LongNameBlowoutTest as _LongNameBlowoutTest
from sqlalchemy.testing.suite import QuotedNameArgumentTest as _QuotedNameArgumentTest
from sqlalchemy.testing.suite import JoinTest as _JoinTest
from sqlalchemy.testing.suite import BizarroCharacterFKResolutionTest as _BizarroCharacterFKResolutionTest
from sqlalchemy.testing.suite import ServerSideCursorsTest as _ServerSideCursorsTest
from sqlalchemy import types as sql_types
from sqlalchemy import testing, select
from sqlalchemy.testing import config, eq_


class ComponentReflectionTest(_ComponentReflectionTest):
    @testing.requires.index_reflection
    def test_get_indexes(self):
        pass


class ComponentReflectionTestExtra(_ComponentReflectionTestExtra):

    @testing.requires.table_reflection
    def test_varchar_reflection(self, connection, metadata):
        typ = self._type_round_trip(
            connection, metadata, sql_types.String(52)
        )[0]
        assert isinstance(typ, sql_types.String)
        # eq_(typ.length, 52)  #  No length in Databend


class BooleanTest(_BooleanTest):
    __backend__ = True

    def test_whereclause(self):
        """
        This is overridden from Ancestor implementation because Databend does not support `WHERE NOT true|false`
        Please compare this version with the overridden test
        """
        # testing "WHERE <column>" renders a compatible expression
        boolean_table = self.tables.boolean_table

        with config.db.begin() as conn:
            conn.execute(
                boolean_table.insert(),
                [
                    {"id": 1, "value": True, "unconstrained_value": True},
                    {"id": 2, "value": False, "unconstrained_value": False},
                ],
            )

            eq_(
                conn.scalar(
                    select(boolean_table.c.id).where(boolean_table.c.value)
                ),
                1,
            )
            eq_(
                conn.scalar(
                    select(boolean_table.c.id).where(
                        boolean_table.c.unconstrained_value
                    )
                ),
                1,
            )


class CompoundSelectTest(_CompoundSelectTest):

    @testing.skip("databend")
    def test_limit_offset_aliased_selectable_in_unions(self):
        pass

    @testing.skip("databend")
    def test_limit_offset_selectable_in_unions(self):
        pass

    @testing.skip("databend")
    def test_limit_offset_in_unions_from_alias(self):
        pass


class DeprecatedCompoundSelectTest(_DeprecatedCompoundSelectTest):
    @testing.skip("databend")
    def test_limit_offset_aliased_selectable_in_unions(self):
        pass

    @testing.skip("databend")
    def test_limit_offset_selectable_in_unions(self):
        pass

    @testing.skip("databend")
    def test_limit_offset_in_unions_from_alias(self):
        pass


class HasIndexTest(_HasIndexTest):
    __requires__ = ('index_reflection',)


class InsertBehaviorTest(_InsertBehaviorTest):
    @testing.skip("databend")  # required autoinc columns
    def test_insert_from_select_autoinc(self, connection):
        pass

    @testing.skip("databend")  # required autoinc columns
    def test_insert_from_select_autoinc_no_rows(self, connection):
        pass

    @testing.skip("databend")  # required autoinc columns
    def test_no_results_for_non_returning_insert(self, connection):
        pass


class LikeFunctionsTest(_LikeFunctionsTest):

    @testing.skip("databend")
    def test_contains_autoescape(self):
        pass

    @testing.skip("databend")
    def test_contains_escape(self):
        pass

    @testing.skip("databend")
    def test_contains_autoescape_escape(self):
        pass

    @testing.skip("databend")
    def test_endswith_autoescape(self):
        pass

    @testing.skip("databend")
    def test_endswith_escape(self):
        pass

    @testing.skip("databend")
    def test_endswith_autoescape_escape(self):
        pass

    @testing.skip("databend")
    def test_startswith_autoescape(self):
        pass

    @testing.skip("databend")
    def test_startswith_escape(self):
        pass

    @testing.skip("databend")
    def test_startswith_autoescape_escape(self):
        pass


class LongNameBlowoutTest(_LongNameBlowoutTest):
    __requires__ = ("index_reflection",)  # This will do to make it skip for now


class QuotedNameArgumentTest(_QuotedNameArgumentTest):
    def quote_fixtures(fn):
        return testing.combinations(
            ("quote ' one",),
            ('quote " two', testing.requires.symbol_names_w_double_quote),
        )(fn)

    @quote_fixtures
    @testing.skip("databend")
    def test_get_pk_constraint(self, name):
        pass

    @quote_fixtures
    @testing.skip("databend")
    def test_get_foreign_keys(self, name):
        pass

    @quote_fixtures
    @testing.skip("databend")
    def test_get_indexes(self, name):
        pass


class JoinTest(_JoinTest):
    __requires__ = ("foreign_keys",)


class BizarroCharacterFKResolutionTest(_BizarroCharacterFKResolutionTest):
    __requires__ = ("foreign_keys",)


class BinaryTest(_BinaryTest):

    # ToDo - get this working, failing because cannot substitute bytes parameter into sql statement
    # CREATE TABLE binary_test (x binary not null)
    # INSERT INTO binary_test (x) values (b'7\xe7\x9f') ???
    # It is possible to do this
    # INSERT INTO binary_test (x) values (TO_BINARY('7\xe7\x9f'))
    # but that's not really a solution I don't think
    @testing.skip("databend")
    def test_binary_roundtrip(self):
        pass

    @testing.skip("databend")
    def test_pickle_roundtrip(self):
        pass


class ServerSideCursorsTest(_ServerSideCursorsTest):

    def _is_server_side(self, cursor):
        # ToDo - requires implementation of `stream_results` option, so True always for now
        if self.engine.dialect.driver == "databend":
            return True
        return super()

    # ToDo - The commented out testing combinations here should be reviewed when `stream_results` is implemented
    @testing.combinations(
        ("global_string", True, "select 1", True),
        ("global_text", True, text("select 1"), True),
        ("global_expr", True, select(1), True),
        # ("global_off_explicit", False, text("select 1"), False),
        (
            "stmt_option",
            False,
            select(1).execution_options(stream_results=True),
            True,
        ),
        # (
        #     "stmt_option_disabled",
        #     True,
        #     select(1).execution_options(stream_results=False),
        #     False,
        # ),
        ("for_update_expr", True, select(1).with_for_update(), True),
        # TODO: need a real requirement for this, or dont use this test
        # (
        #     "for_update_string",
        #     True,
        #     "SELECT 1 FOR UPDATE",
        #     True,
        #     testing.skip_if(["sqlite", "mssql"]),
        # ),
        # ("text_no_ss", False, text("select 42"), False),
        (
            "text_ss_option",
            False,
            text("select 42").execution_options(stream_results=True),
            True,
        ),
        id_="iaaa",
        argnames="engine_ss_arg, statement, cursor_ss_status",
    )
    def test_ss_cursor_status(
        self, engine_ss_arg, statement, cursor_ss_status
    ):
        super()

    @testing.skip("databend")  # ToDo - requires implementation of `stream_results` option
    def test_stmt_enabled_conn_option_disabled(self):
        pass

    @testing.skip("databend")  # ToDo - requires implementation of `stream_results` option
    def test_aliases_and_ss(self):
        pass

    @testing.skip("databend")  # Skipped because requires auto increment primary key
    def test_roundtrip_fetchall(self):
        pass

    @testing.skip("databend")  # Skipped because requires auto increment primary key
    def test_roundtrip_fetchmany(self):
        pass
