"""
CUBRIDPy - A DB API v2.0 compatible driver interface for CUBRID (www.cubrid.org).
"""

#Integer constant stating the level of thread safety the interface supports.
#Possible values are:
#0     Threads may not share the module.
#1     Threads may share the module, but not connections.
#2     Threads may share the module and connections.
#3     Threads may share the module, connections and cursors.
threadsafety = 2

#String constant stating the supported DB API level.
#Currently only the strings '1.0' and '2.0' are allowed.
#If not given, a DB-API 1.0 level interface should be assumed.
apilevel = '2.0'

#String constant stating the type of parameter marker formatting expected by the interface.
#Possible values are:
#'qmark'         Question mark style, e.g. '...WHERE name=?'
#'numeric'       Numeric, positional style, e.g. '...WHERE name=:1'
#'named'         Named style, e.g. '...WHERE name=:name'
#'format'        ANSI C printf format codes, e.g. '...WHERE name=%s'
#'pyformat'      Python extended format codes, e.g. '...WHERE name=%(name)s'
paramstyle = 'qmark'

from CUBRIDPy.CUBRIDConnection import CUBRIDConnection
from CUBRIDPy.errors import (
    Error, Warning, InterfaceError, DatabaseError,
    NotSupportedError, DataError, IntegrityError, ProgrammingError,
    OperationalError, InternalError)

def Connect(*args):
    """
    Shortcut for creating a CUBRIDPy.CUBRIDConnection object.
    """
    return CUBRIDConnection(*args)

connect = Connect

