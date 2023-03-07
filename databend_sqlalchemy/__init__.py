#!/usr/bin/env python


from databend_sqlalchemy.entry_points import validate_entrypoints

driver_name = 'clickhousedb'

VERSION = (0, 1, 8)
__version__ = '.'.join(str(x) for x in VERSION)
