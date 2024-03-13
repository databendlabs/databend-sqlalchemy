# tests/test_suite.py

from sqlalchemy.testing.suite import *

from sqlalchemy.testing.suite import ComponentReflectionTestExtra as _ComponentReflectionTestExtra
from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest
from sqlalchemy.testing.suite import DeprecatedCompoundSelectTest as _DeprecatedCompoundSelectTest
from sqlalchemy.testing.suite import BooleanTest as _BooleanTest
from sqlalchemy.testing.suite import CompoundSelectTest as _CompoundSelectTest
from sqlalchemy.testing.suite import HasIndexTest as _HasIndexTest
from sqlalchemy.testing.suite import InsertBehaviorTest as _InsertBehaviorTest
from sqlalchemy.testing.suite import LikeFunctionsTest as _LikeFunctionsTest
from sqlalchemy.testing.suite import LongNameBlowoutTest as _LongNameBlowoutTest
from sqlalchemy.testing.suite import QuotedNameArgumentTest as _QuotedNameArgumentTest
from sqlalchemy.testing.suite import JoinTest as _JoinTest
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
            # Databend does not support `WHERE NOT true|false`
            # eq_(
            #     conn.scalar(
            #         select(boolean_table.c.id).where(~boolean_table.c.value)
            #     ),
            #     2,
            # )
            # eq_(
            #     conn.scalar(
            #         select(boolean_table.c.id).where(
            #             ~boolean_table.c.unconstrained_value
            #         )
            #     ),
            #     2,
            # )


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
    @testing.skip("databend")
    def test_insert_from_select_autoinc(self, connection):
        pass

    @testing.skip("databend")
    def test_insert_from_select_autoinc_no_rows(self, connection):
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
