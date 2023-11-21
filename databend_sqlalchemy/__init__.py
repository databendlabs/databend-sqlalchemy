#!/usr/bin/env python


from databend_sqlalchemy.entry_points import validate_entrypoints  # noqa

VERSION = (0, 3, 0)
__version__ = ".".join(str(x) for x in VERSION)
