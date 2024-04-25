#!/usr/bin/env python
#
# Note: parts of the file come from https://github.com/snowflakedb/snowflake-sqlalchemy
#       licensed under the same Apache 2.0 License

from sqlalchemy.sql.selectable import Select, Subquery, TableClause
from sqlalchemy.sql.dml import UpdateBase
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.expression import select


class _OnMergeBaseClause(ClauseElement):
    # __visit_name__ = "on_merge_base_clause"

    def __init__(self):
        self.set = {}
        self.predicate = None

    def __repr__(self):
        return f" AND {str(self.predicate)}" if self.predicate is not None else ""

    def values(self, **kwargs):
        self.set = kwargs
        return self

    def where(self, expr):
        self.predicate = expr
        return self


class WhenMergeMatchedUpdateClause(_OnMergeBaseClause):
    __visit_name__ = "when_merge_matched_update"

    def __repr__(self):
        case_predicate = super()
        update_str = f"WHEN MATCHED{case_predicate} THEN UPDATE"
        if not self.set:
            return f"{update_str} *"

        set_values = ", ".join([f"{set_item[0]} = {set_item[1]}" for set_item in self.set.items()])
        return f"{update_str} SET {str(set_values)}"


class WhenMergeMatchedDeleteClause(_OnMergeBaseClause):
    __visit_name__ = "when_merge_matched_delete"

    def __repr__(self):
        case_predicate = super()
        return f"WHEN MATCHED{case_predicate} THEN DELETE"


class WhenMergeUnMatchedClause(_OnMergeBaseClause):
    __visit_name__ = "when_merge_unmatched"

    def __repr__(self):
        case_predicate = super()
        insert_str = f"WHEN NOT MATCHED{case_predicate} THEN INSERT"
        if not self.set:
            return f"{insert_str} *"

        sets, sets_tos = zip(*self.set.items())
        return "{} ({}) VALUES ({})".format(
            insert_str,
            ", ".join(sets),
            ", ".join(map(str, sets_tos)),
        )


class Merge(UpdateBase):
    __visit_name__ = "merge"
    _bind = None

    def __init__(self, target, source, on):
        if not isinstance(source, (TableClause, Select, Subquery)):
            raise Exception(f'Invalid type for merge source: {source}')
        self.target = target
        self.source = source
        self.on = on
        self.clauses = []

    def __repr__(self):
        clauses = " ".join([repr(clause) for clause in self.clauses])
        return f"MERGE INTO {self.target} USING ({select(self.source)}) AS {self.source.name} ON {self.on}" + (
            f" {clauses}" if clauses else ""
        )

    def when_matched_then_update(self):
        clause = WhenMergeMatchedUpdateClause()
        self.clauses.append(clause)
        return clause

    def when_matched_then_delete(self):
        clause = WhenMergeMatchedDeleteClause()
        self.clauses.append(clause)
        return clause

    def when_not_matched_then_insert(self):
        clause = WhenMergeUnMatchedClause()
        self.clauses.append(clause)
        return clause
