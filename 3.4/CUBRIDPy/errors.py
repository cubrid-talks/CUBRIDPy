"""
This module implements Exception classes
http://www.python.org/dev/peps/pep-0249/
"""
class Error(StandardError):
    """
    Base class for all driver error exceptions
    """

    def __init__(self, err_id = None, err_msg = None):
        self._err_id = err_id or -1
        self._err_msg = err_msg

    #def __str__(self):
    #    return '%d: %s' % (self._err_id, self._err_msg)

class Warning(StandardError):
    """
    Warnings exception
    """
    pass

class InterfaceError(Error):
    """
    Exception for interface errors
    """
    pass

class DatabaseError(Error):
    """
    Exception for database errors
    """
    pass

class InternalError(DatabaseError):
    """
    Exception for internal errors
    """
    pass

class OperationalError(DatabaseError):
    """
    Exception for database operations errors
    """
    pass

class ProgrammingError(DatabaseError):
    """
    Exception for programming errors
    """
    pass

class IntegrityError(DatabaseError):
    """
    Exception for data relational integrity errors
    """
    pass

class DataError(DatabaseError):
    """
    Exception for data errors
    """
    pass

class NotSupportedError(DatabaseError):
    """
    Exception for unsupported database operations
    """
    pass
