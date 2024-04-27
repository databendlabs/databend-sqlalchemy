#!/usr/bin/env python

from sqlalchemy import Column
from sqlalchemy import exc
from sqlalchemy import Integer
from sqlalchemy import schema
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import types as sqltypes
# from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.testing import config
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import assert_raises
from sqlalchemy.testing.assertions import eq_

from databend_sqlalchemy.databend_dialect import Merge


class MergeIntoTest(fixtures.TablesTest):
    __backend__ = True
    run_define_tables = "each"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
            Column("login_email", String(50)),
        )

        Table(
            "users_schema",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
            schema=config.test_schema,
        )

        Table(
            "users_xtra",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
            Column("login_email", String(50)),
        )


    def test_no_action_raises(self, connection):
        users = self.tables.users
        users_xtra = self.tables.users_xtra

        connection.execute(users.insert(), dict(id=1, name="name1", login_email="email1"))
        connection.execute(users.insert(), dict(id=2, name="name2", login_email="email2"))
        connection.execute(users_xtra.insert(), dict(id=1, name="newname1", login_email="newemail1"))
        connection.execute(users_xtra.insert(), dict(id=2, name="newname2", login_email="newemail2"))


        merge = Merge(users, users_xtra, users.c.id == users_xtra.c.id)

        assert_raises(
            exc.DBAPIError,
            connection.execute,
            merge,
        )

    def test_select_as_source(self, connection):
        users = self.tables.users
        users_xtra = self.tables.users_xtra

        connection.execute(users.insert(), dict(id=1, name="name1", login_email="email1"))
        connection.execute(users_xtra.insert(), dict(id=1, name="newname1", login_email="newemail1"))
        connection.execute(users_xtra.insert(), dict(id=2, name="newname2", login_email="newemail2"))

        select_source = users_xtra.select().where(users_xtra.c.id != 99)
        merge = Merge(users, select_source, users.c.id == select_source.selected_columns.id)
        merge.when_matched_then_update().values(name=select_source.selected_columns.name)
        merge.when_not_matched_then_insert()

        result = connection.execute(merge)
        eq_(
            connection.execute(
                users.select().order_by(users.c.id)
            ).fetchall(),
            [(1, "newname1", "email1"), (2, "newname2", "newemail2")],
        )

    def test_alias_as_source(self, connection):
        users = self.tables.users
        users_xtra = self.tables.users_xtra

        connection.execute(users.insert(), dict(id=1, name="name1", login_email="email1"))
        connection.execute(users_xtra.insert(), dict(id=1, name="newname1", login_email="newemail1"))
        connection.execute(users_xtra.insert(), dict(id=2, name="newname2", login_email="newemail2"))

        alias_source = users_xtra.select().where(users_xtra.c.id != 99).alias('x')
        merge = Merge(users, alias_source, users.c.id == alias_source.c.id)
        merge.when_matched_then_update().values(name=alias_source.c.name)
        merge.when_not_matched_then_insert()

        result = connection.execute(merge)
        eq_(
            connection.execute(
                users.select().order_by(users.c.id)
            ).fetchall(),
            [(1, "newname1", "email1"), (2, "newname2", "newemail2")],
        )

    def test_subquery_as_source(self, connection):
        users = self.tables.users
        users_xtra = self.tables.users_xtra

        connection.execute(users.insert(), dict(id=1, name="name1", login_email="email1"))
        connection.execute(users_xtra.insert(), dict(id=1, name="newname1", login_email="newemail1"))
        connection.execute(users_xtra.insert(), dict(id=2, name="newname2", login_email="newemail2"))

        subquery_source = users_xtra.select().where(users_xtra.c.id != 99).subquery()
        merge = Merge(users, subquery_source, users.c.id == subquery_source.c.id)
        merge.when_matched_then_update().values(name=subquery_source.c.name)
        merge.when_not_matched_then_insert()

        result = connection.execute(merge)
        eq_(
            connection.execute(
                users.select().order_by(users.c.id)
            ).fetchall(),
            [(1, "newname1", "email1"), (2, "newname2", "newemail2")],
        )

    def test_when_not_matched_insert(self, connection):
        users = self.tables.users
        users_xtra = self.tables.users_xtra

        connection.execute(users.insert(), dict(id=1, name="name1", login_email="email1"))
        connection.execute(users_xtra.insert(), dict(id=1, name="newname1", login_email="newemail1"))
        connection.execute(users_xtra.insert(), dict(id=2, name="newname2", login_email="newemail2"))

        merge = Merge(users, users_xtra, users.c.id == users_xtra.c.id)
        merge.when_not_matched_then_insert()

        result = connection.execute(merge)
        eq_(
            connection.execute(
                users.select().order_by(users.c.id)
            ).fetchall(),
            [(1, "name1", "email1"), (2, "newname2", "newemail2")],
        )

    def test_when_matched_update(self, connection):
        users = self.tables.users
        users_xtra = self.tables.users_xtra

        connection.execute(users.insert(), dict(id=1, name="name1", login_email="email1"))
        connection.execute(users.insert(), dict(id=2, name="name2", login_email="email2"))
        connection.execute(users_xtra.insert(), dict(id=1, name="newname1", login_email="newemail1"))

        merge = Merge(users, users_xtra, users.c.id == users_xtra.c.id)
        merge.when_matched_then_update()

        result = connection.execute(merge)
        eq_(
            connection.execute(
                users.select().order_by(users.c.id)
            ).fetchall(),
            [(1, "newname1", "newemail1"), (2, "name2", "email2")],
        )

    def test_when_matched_update_column(self, connection):
        users = self.tables.users
        users_xtra = self.tables.users_xtra

        connection.execute(users.insert(), dict(id=1, name="name1", login_email="email1"))
        connection.execute(users.insert(), dict(id=2, name="name2", login_email="email2"))
        connection.execute(users_xtra.insert(), dict(id=1, name="newname1", login_email="newemail1"))

        merge = Merge(users, users_xtra, users.c.id == users_xtra.c.id)
        merge.when_matched_then_update().values(name=users_xtra.c.name)

        result = connection.execute(merge)
        eq_(
            connection.execute(
                users.select().order_by(users.c.id)
            ).fetchall(),
            [(1, "newname1", "email1"), (2, "name2", "email2")],
        )

    def test_when_matched_update_criteria(self, connection):
        users = self.tables.users
        users_xtra = self.tables.users_xtra

        connection.execute(users.insert(), dict(id=1, name="name1", login_email="email1"))
        connection.execute(users.insert(), dict(id=2, name="name2", login_email="email2"))
        connection.execute(users_xtra.insert(), dict(id=1, name="newname1", login_email="newemail1"))
        connection.execute(users_xtra.insert(), dict(id=2, name="newname2", login_email="newemail2"))

        merge = Merge(users, users_xtra, users.c.id == users_xtra.c.id)
        merge.when_matched_then_update().where(users_xtra.c.id != 1)

        result = connection.execute(merge)
        eq_(
            connection.execute(
                users.select().order_by(users.c.id)
            ).fetchall(),
            [(1, "name1", "email1"), (2, "newname2", "newemail2")],
        )

    def test_when_matched_update_criteria_column(self, connection):
        users = self.tables.users
        users_xtra = self.tables.users_xtra

        connection.execute(users.insert(), dict(id=1, name="name1", login_email="email1"))
        connection.execute(users.insert(), dict(id=2, name="name2", login_email="email2"))
        connection.execute(users_xtra.insert(), dict(id=1, name="newname1", login_email="newemail1"))
        connection.execute(users_xtra.insert(), dict(id=2, name="newname2", login_email="newemail2"))

        merge = Merge(users, users_xtra, users.c.id == users_xtra.c.id)
        merge.when_matched_then_update().where(users_xtra.c.id != 1).values(name=users_xtra.c.name)

        result = connection.execute(merge)
        eq_(
            connection.execute(
                users.select().order_by(users.c.id)
            ).fetchall(),
            [(1, "name1", "email1"), (2, "newname2", "email2")],
        )

    def test_when_matched_delete(self, connection):
        users = self.tables.users
        users_xtra = self.tables.users_xtra

        connection.execute(users.insert(), dict(id=1, name="name1", login_email="email1"))
        connection.execute(users.insert(), dict(id=2, name="name2", login_email="email2"))
        connection.execute(users_xtra.insert(), dict(id=2, name="newname2", login_email="newemail2"))

        merge = Merge(users, users_xtra, users.c.id == users_xtra.c.id)
        merge.when_matched_then_delete()

        result = connection.execute(merge)
        eq_(
            connection.execute(
                users.select().order_by(users.c.id)
            ).fetchall(),
            [(1, "name1", "email1")],
        )

    def test_mixed_criteria(self, connection):
        users = self.tables.users
        users_xtra = self.tables.users_xtra

        connection.execute(users.insert(), dict(id=1, name="name1", login_email="email1"))
        connection.execute(users.insert(), dict(id=2, name="name2", login_email="email2"))
        connection.execute(users_xtra.insert(), dict(id=1, name="newname1", login_email="newemail1"))
        connection.execute(users_xtra.insert(), dict(id=2, name="newname2", login_email="newemail2"))
        connection.execute(users_xtra.insert(), dict(id=3, name="newname3", login_email="newemail3"))

        merge = Merge(users, users_xtra, users.c.id == users_xtra.c.id)
        merge.when_matched_then_update().where(users_xtra.c.id == 1)
        merge.when_matched_then_delete()
        merge.when_not_matched_then_insert()

        result = connection.execute(merge)
        eq_(
            connection.execute(
                users.select().order_by(users.c.id)
            ).fetchall(),
            [(1, "newname1", "newemail1"), (3, "newname3", "newemail3")],
        )

    def test_no_matches(self, connection):
        users = self.tables.users
        users_xtra = self.tables.users_xtra

        connection.execute(users.insert(), dict(id=1, name="name1", login_email="email1"))
        connection.execute(users.insert(), dict(id=2, name="name2", login_email="email2"))
        connection.execute(users_xtra.insert(), dict(id=1, name="newname1", login_email="newemail1"))
        connection.execute(users_xtra.insert(), dict(id=2, name="newname2", login_email="newemail2"))


        merge = Merge(users, users_xtra, users.c.id == users_xtra.c.id)
        merge.when_not_matched_then_insert().where(users_xtra.c.id == 99)

        result = connection.execute(merge)
        eq_(
            connection.execute(
                users.select().order_by(users.c.id)
            ).fetchall(),
            [(1, "name1", "email1"), (2, "name2", "email2")],
        )
    #
    # def test_selectable_source_when_not_matched_insert(self, connection):
    #     users = self.tables.users
    #     users_xtra = self.tables.users_xtra
    #
    #     connection.execute(users.insert(), dict(id=1, name="name1", login_email="email1"))
    #     connection.execute(users_xtra.insert(), dict(id=1, name="newname1", login_email="newemail1"))
    #     connection.execute(users_xtra.insert(), dict(id=2, name="newname2", login_email="newemail2"))
    #
    #     merge = Merge(users, users_xtra.select(), users.c.id == users_xtra.c.id)
    #     merge.when_not_matched_then_insert()
    #
    #     result = connection.execute(merge)
    #     eq_(
    #         connection.execute(
    #             users.select().order_by(users.c.id)
    #         ).fetchall(),
    #         [(1, "name1", "email1"), (2, "newname2", "newemail2")],
    #     )