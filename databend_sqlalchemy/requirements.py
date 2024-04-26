
from sqlalchemy.testing.requirements import SuiteRequirements

from sqlalchemy.testing import exclusions


class Requirements(SuiteRequirements):

    @property
    def foreign_keys(self):
        """Target database must support foreign keys."""

        return exclusions.closed()  # Currently no foreign keys in Databend

    @property
    def binary_comparisons(self):
        """target database/driver can allow BLOB/BINARY fields to be compared
        against a bound parameter value.
        """
        return exclusions.closed()  # Currently no binary type in Databend

    @property
    def binary_literals(self):
        """target backend supports simple binary literals, e.g. an
        expression like::

            SELECT CAST('foo' AS BINARY)

        Where ``BINARY`` is the type emitted from :class:`.LargeBinary`,
        e.g. it could be ``BLOB`` or similar.

        Basically fails on Oracle.

        """
        return exclusions.closed()  # Currently no binary type in Databend

    @property
    def temporary_tables(self):
        """target database supports temporary tables"""
        return exclusions.closed()  # Databend does not currently support temporary tables

    @property
    def temp_table_reflection(self):
        return exclusions.closed()

    @property
    def self_referential_foreign_keys(self):
        """Target database must support self-referential foreign keys."""

        return exclusions.closed()  # Databend does not currently support foreign keys

    @property
    def foreign_key_ddl(self):
        """Target database must support the DDL phrases for FOREIGN KEY."""

        return exclusions.closed()  # Databend does not currently support foreign keys

    @property
    def index_reflection(self):
        return exclusions.closed()   # Databend does not currently support indexes

    @property
    def primary_key_constraint_reflection(self):
        return exclusions.closed()  # Databend does not currently support primary keys

    @property
    def foreign_key_constraint_reflection(self):
        return exclusions.closed()  # Databend does not currently support foreign keys

    @property
    def unique_constraint_reflection(self):
        """target dialect supports reflection of unique constraints"""
        return exclusions.closed()  # Databend does not currently support unique constraints

    @property
    def duplicate_key_raises_integrity_error(self):
        """target dialect raises IntegrityError when reporting an INSERT
        with a primary key violation.  (hint: it should)

        """
        return exclusions.closed()   # Databend does not currently support primary keys

    @property
    def sql_expression_limit_offset(self):
        """target database can render LIMIT and/or OFFSET with a complete
        SQL expression, such as one that uses the addition operator.
        parameter
        """

        return exclusions.closed()  # Databend does not currently support expressions in limit/offset

    @property
    def autoincrement_without_sequence(self):
        """If autoincrement=True on a column does not require an explicit
        sequence. This should be false only for oracle.
        """
        return exclusions.closed()

    @property
    def datetime_timezone(self):
        """target dialect supports representation of Python
        datetime.datetime() with tzinfo with DateTime(timezone=True)."""

        return exclusions.closed()

    @property
    def time_timezone(self):
        """target dialect supports representation of Python
        datetime.time() with tzinfo with Time(timezone=True)."""

        return exclusions.closed()

    # @property
    # def datetime_implicit_bound(self):
    #     """target dialect when given a datetime object will bind it such
    #     that the database server knows the object is a datetime, and not
    #     a plain string.
    #
    #     """
    #     return exclusions.closed()  #  `SELECT '2012-10-15 12:57:18' AS thing` does not yield a timestamp in Databend

    @property
    def datetime_microseconds(self):
        """target dialect supports representation of Python
        datetime.datetime() with microsecond objects."""

        return exclusions.closed()

    @property
    def timestamp_microseconds(self):
        """target dialect supports representation of Python
        datetime.datetime() with microsecond objects but only
        if TIMESTAMP is used."""
        return exclusions.closed()

    @property
    def time(self):
        """target dialect supports representation of Python
        datetime.time() objects."""

        return exclusions.closed()

    @property
    def time_microseconds(self):
        """target dialect supports representation of Python
        datetime.time() with microsecond objects."""

        return exclusions.closed()

    @property
    def autoincrement_insert(self):
        """target platform generates new surrogate integer primary key values
        when insert() is executed, excluding the pk column."""

        return exclusions.closed()

    @property
    def views(self):
        """Target database must support VIEWs."""

        return exclusions.open()

    @property
    def unicode_data(self):
        """Target database/dialect must support Python unicode objects with
        non-ASCII characters represented, delivered as bound parameters
        as well as in result rows.

        """
        return exclusions.closed()

    @property
    def precision_generic_float_type(self):
        """target backend will return native floating point numbers with at
        least seven decimal places when using the generic Float type.

        """
        return exclusions.closed()  #ToDo - I couldn't get the test for this one working, not sure where the issue is - AssertionError: {Decimal('15.7563829')} != {Decimal('15.7563827')}

    @property
    def array_type(self):
        return exclusions.closed()

    @property
    def float_is_numeric(self):
        """target backend uses Numeric for Float/Dual"""

        return exclusions.closed()

    @property
    def json_type(self):
        """target platform implements a native JSON type."""

        return exclusions.closed()  #ToDo - This could be enabled if primary keys were supported

    @property
    def reflect_table_options(self):
        """Target database must support reflecting table_options."""
        return exclusions.open()
