"""
Cursor class
http://www.python.org/dev/peps/pep-0249/
"""
import weakref
from CUBRIDPy.localization.ERROR_ID import ERROR_ID
from CUBRIDPy.localization.localization import get_error_message
from CUBRIDPy.protocol.CloseQuery import *
from CUBRIDPy.protocol.PrepareAndExecuteQuery import *
from CUBRIDPy.protocol.Fetch import *
from CUBRIDPy.protocol.BatchExecute import *
from CUBRIDPy.protocol.LOBRead import *
import errors
import logging

class CUBRIDCursor(object):
    """
    This class is a skeleton and defines methods and members,
    as required for the Python Database API Specification v2.0 (PEP-249)
    """

    def __init__(self, connection = None):
        # This attribute will be None if the cursor has not had an operation invoked via the .execute*() method yet.
        self._description = None
        self._rowcount = 0 # affected rows
        self._last_insert_id = None
        self._connection = None
        # Public variables
        self.arraysize = 1
        self.query_handle = -1
        #internal query data results
        self._query_results = None
        # For now, these are the rules:
        # 0: No position
        # 1: First row
        # N: last row
        # for SELECT with results, values are [1...N]
        self._query_cursor_pos = 0
        self._rows_count = 0 # current tuple count
        self._total_rows_count = 0
        self._query_metadata = None
        self._array_size = 1
        self.query_executed = False
        self._query_packet = None
        self._last_query = None
        self._last_query = None
        self._cursor_closed = False
        # Error information
        self.error_code = 0
        self.error_msg = ''
        self.batch_execute_error_codes = []
        self.batch_execute_error_msgs = []

        if connection is not None:
            self._set_connection(connection)
        pass

    def _reset_query_data(self):
        """
        Resets internal variables
        """
        self._rowcount = 0 # affected rows
        self._query_results = None
        self._query_cursor_pos = 0
        self._rows_count = 0 # current tuple count
        self._total_rows_count = 0
        self._query_metadata = None
        self._last_insert_id = None
        self._description = None
        self._last_query = None
        self._cursor_closed = False
        self._clear_error()

    def _clear_error(self):
        """
        Clears the error information
        """
        self.error_code = 0
        self.error_msg = ''
        self.batch_execute_error_codes = []
        self.batch_execute_error_msgs = []

    def _set_error(self, error_code, error_msg):
        """
        Initializes the error information
        """
        self.error_code = error_code
        self.error_msg = error_msg

    def __iter__(self):
        """
        Iteration over the result set which calls self.fetchone() and returns the next row.
        """
        return iter(self.fetchone, None)

    def _set_connection(self, connection):
        try:
            from weakref import proxy

            self._connection = weakref.proxy(connection)
        except Exception as ex:
            logging.error(str(ex))
            raise

    def _reset_result(self):
        self._rowcount = 0
        self._lastrowid = None
        self._description = None
        self._clear_error()

    @property
    def description(self):
        """
        This read-only attribute is a sequence of 7-item sequences.
        Each of these sequences contains information describing one result column:
            (name,
             type_code,
             display_size,
             internal_size,
             precision,
             scale,
             null_ok)
        The first two items (name and type_code) are mandatory,
        the other five are optional and are set to None if no
        meaningful values can be provided.

        This attribute will be None for operations that
        do not return rows or if the cursor has not had an
        operation invoked via the .execute*() method yet.
        """
        return self._description

    def callproc(self, procedure_name, parameters = ()):
        """
        Call a stored database procedure with the given name.
        The sequence of parameters must contain one entry for each
        argument that the procedure expects.
        The result of the call is returned as modified copy of the input sequence.
        Input parameters are left untouched, output and
        input/output parameters replaced with possibly new values.
        """
        raise AttributeError(get_error_message(self._connection._locale, ERROR_ID.ERROR_NOT_IMPLEMENTED))

    @property
    def rowcount(self):
        """
        Returns the number of rows produced or affected

        This property returns the number of rows produced by queries
        such as a SELECT, or affected rows when executing DML statements
        like INSERT or UPDATE.

        Note that for non-buffered cursors it is impossible to know the
        number of rows produced before having fetched them all. For those,
        the number of rows will be -1 right after execution, and
        incremented when fetching rows.

        Returns an integer.
        """
        return self._rowcount

    @property
    def lastrowid(self):
        """
        Returns the value generated for an AUTO_INCREMENT column

        Returns the value generated for an AUTO_INCREMENT column by
        the previous INSERT or UPDATE statement or None when there is
        no such value available.

        Returns a long value or None.
        """
        return self._last_insert_id

    def close(self):
        """
        Close the cursor

        Returns True when successful, otherwise False.
        """
        if self._connection is None:
            return False
        if self._cursor_closed:
            return True
        if not self.query_executed:
            return None

        self._reset_result()
        # self._connection = None
        self.query_executed = False

        _CloseQuery = None
        try:
            self._clear_error()
            # Close connection
            _CloseQuery = CloseQuery(self._connection._socket, self._connection._CAS_INFO, self.query_handle,
                self._connection._locale)
            _CloseQuery.SendRequest()
            _CloseQuery.GetResponse()
        except Exception:
            if _CloseQuery.error_code != 0:
                self._set_error(_CloseQuery.error_code, _CloseQuery.error_msg)
                raise Exception("%d: %s" % (self._connection.errorCode, self._connection.errorMsg))
            else:
                raise

        self._cursor_closed = True

        return _CloseQuery.error_code == 0

    def __del__(self):
        if not self._cursor_closed:
            self.close()

    def batch_execute(self, SQLs):
        """
        Execute batch SQL statements.
        Returns count of executed statements.
        """
        if self._connection is None:
            return None
        if self._cursor_closed:
            raise errors.Error(ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED,
                get_error_message(self._connection._locale, ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED))
        if SQLs is None:
            return None

        _BatchExecute = None

        try:
            self._reset_query_data()

            _BatchExecute = BatchExecute(self._connection._socket,
                self._connection._CAS_INFO,
                SQLs,
                self._connection._auto_commit,
                self._connection._locale)
            _BatchExecute.SendRequest()
            _BatchExecute.GetResponse()
            self.batch_execute_error_codes = _BatchExecute.results_error_codes
            self.batch_execute_error_msgs = _BatchExecute.results_error_msgs
            self._rowcount = _BatchExecute.executed_count

            return _BatchExecute.executed_count
        except Exception as ex:
            logging.error(str(ex))
            if _BatchExecute.error_code != 0:
                self._set_error(_BatchExecute.error_code, _BatchExecute.error_msg)
                self.batch_execute_error_codes = _BatchExecute.results_error_codes
                self.batch_execute_error_msgs = _BatchExecute.results_error_msgs
                raise Exception("%d: %s" % (self.error_code, self.error_msg))
            else:
                logging.error(str(ex))
                raise

    def execute(self, query, args = None):
        """Execute a query.
        query -- string, query to execute on server
        args -- optional sequence or mapping, parameters to use with query.
        Note: If args is a sequence, then %s must be used as the
        parameter placeholder in the query. If a mapping is used,
        %(key)s must be used as the placeholder.
        Returns rows count, for SELECT statements
        """
        if self._connection is None:
            return None
        if self._cursor_closed:
            raise errors.Error(ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED,
                get_error_message(self._connection._locale, ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED))
        if query is None:
            return None

        _PrepareAndExecuteQuery = None
        try:
            self._reset_query_data()

            if args is not None:
                _query = query % tuple(args)
            else:
                _query = query

            self._last_query = _query # Save last executed query

            # If query is not SELECT, deffer execution to Batch Execute method
            if not _query.strip().lower().startswith('select '):
                # This attribute will be None for operations that do not return rows
                # or if the cursor has not had an operation invoked via the .execute*() method yet.
                self._description = None
                return self.batch_execute([_query])

            self._query_results = None

            _PrepareAndExecuteQuery = PrepareAndExecuteQuery(self._connection._socket,
                self._connection._CAS_INFO,
                _query,
                self._connection._auto_commit,
                self._connection._locale)
            _PrepareAndExecuteQuery.SendRequest()
            _PrepareAndExecuteQuery.GetResponse()
            self.query_executed = True
            self.query_handle = _PrepareAndExecuteQuery.query_handle
            self._query_results = _PrepareAndExecuteQuery.results
            self._description = _PrepareAndExecuteQuery.description
            self._total_rows_count = _PrepareAndExecuteQuery._total_tuple_count
            self._rows_count = _PrepareAndExecuteQuery._tuple_count
            self._query_packet = _PrepareAndExecuteQuery
            self._rowcount = _PrepareAndExecuteQuery._total_tuple_count

            return self._rowcount
        except TypeError, ex:
            logging.error('TypeError: ' + str(ex))
            raise
        except Exception:
            self._set_error(_PrepareAndExecuteQuery.error_code, _PrepareAndExecuteQuery.error_msg)
            raise

    def read_lob(self, lob_handle, lob_type, read_position, length_to_read):
        if self._connection is None:
            return None
        if self._cursor_closed:
            raise errors.Error(ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED,
                get_error_message(self._connection._locale, ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED))

        _LOBRead = None

        try:
            _LOBRead = LOBRead(self._connection._socket,
                self._connection._CAS_INFO,
                lob_handle,
                lob_type,
                read_position,
                length_to_read
            )
            _LOBRead.SendRequest()
            _LOBRead.GetResponse()
            self.query_executed = True
            self._query_packet = _LOBRead
            lob_buffer = _LOBRead.lob_buffer

            return lob_buffer
        except TypeError, ex:
            logging.error('TypeError: ' + str(ex))
            raise
        except Exception:
            self._set_error(_LOBRead.error_code, _LOBRead.error_msg)
            raise

    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a
        single sequence, or None when no more data is available. [6]
        """
        if self._cursor_closed:
            raise errors.Error(ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED,
                get_error_message(self._connection._locale, ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED))
        if not self.query_executed:
            return None
        if self._query_results is None:
            return None

        _Fetch = None
        try:
            self._query_cursor_pos += 1

            if self._query_cursor_pos > len(self._query_results):
                if self._query_cursor_pos <= self._total_rows_count:
                    _Fetch = Fetch(self._connection._socket,
                        self._connection._CAS_INFO,
                        self.query_handle,
                        self._query_packet,
                        self._rows_count,
                        self._connection._locale)

                    _Fetch.SendRequest()
                    _Fetch.GetResponse()
                    self._query_results += _Fetch._fetch_results
                    self._rows_count += len(_Fetch._fetch_results)

                    return self._query_results[self._query_cursor_pos - 1]
                else:
                    self._query_cursor_pos = 0 # reset cursor position
                    return None

            return self._query_results[self._query_cursor_pos - 1]
        except IndexError:
            return None
        except StopIteration:
            return None
        except Exception:
            self._set_error(_Fetch.error_code, _Fetch.error_msg)
            raise

    def executemany(self, query, args):
        """Execute a multi-row query.
        query: string, query to execute on server
        args: Sequence of sequences or mappings, parameters to use with query.
        Returns array with rows affected
        """
        if self._connection is None:
            return None
        if self._cursor_closed:
            raise errors.Error(ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED,
                get_error_message(self._connection._locale, ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED))

        rows_count = []
        if isinstance(query, (list, tuple)):
            if args is not None:
                for (qry, arg) in zip(query, args):
                    rows_count.append(self.execute(qry, arg))
            else:
                for qry in query:
                    rows_count.append(self.execute(qry))
        else:
            if args is not None:
                rows_count.append(self.execute(query, args))
            else:
                rows_count.append(self.execute(query))

        return rows_count

    def fetchoneassoc(self):
        """
        Fetch the next row of a query result set, returning a
        single associative array/dictionary, or None when no more data is available.
        """
        data = self.fetchone()
        if data is None:
            return None
        desc = self.description
        dict = {}
        for (name, value) in zip(desc, data):
            dict[name[0]] = value

        return dict

    def fetchmany(self, size = 1):
        """
        Fetch the next set of rows of a query result, returning a
        sequence of sequences (e.g. a list of tuples). An empty
        sequence is returned when no more rows are available.
        """
        if self._connection is None:
            return None
        if self._cursor_closed:
            raise errors.Error(ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED,
                get_error_message(self._connection._locale, ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED))
        if not self.query_executed:
            raise errors.Error(ERROR_ID.ERROR_NO_ACTIVE_QUERY,
                get_error_message(self._connection._locale, ERROR_ID.ERROR_NO_ACTIVE_QUERY))
        if self._query_results is None:
            return None

        _Fetch = None
        try:
            start_query_cursor_pos = self._query_cursor_pos
            self._query_cursor_pos += size

            while self._query_cursor_pos >= len(self._query_results):
                _Fetch = Fetch(self._connection._socket,
                    self._connection._CAS_INFO,
                    self.query_handle,
                    self._query_packet,
                    self._rows_count,
                    self._connection._locale)

                _Fetch.SendRequest()
                _Fetch.GetResponse()
                self._query_results += _Fetch._fetch_results
                self._rows_count += len(_Fetch._fetch_results)

                # check if there are more results
                if len(self._query_results) == self._total_rows_count:
                    break

                self._query_cursor_pos += len(_Fetch._fetch_results)

            if self._query_cursor_pos < self._total_rows_count:
                return self._query_results[start_query_cursor_pos:self._query_cursor_pos]
            else:
                self._query_cursor_pos = self._total_rows_count
                return self._query_results[start_query_cursor_pos:self._total_rows_count]

        except IndexError:
            return None
        except StopIteration:
            return None
        except Exception:
            self._set_error(_Fetch.error_code, _Fetch.error_msg)
            raise

    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning
        them as a sequence of sequences (e.g. a list of tuples).
        Note that the cursor's arraysize attribute can affect the
        performance of this operation.
        """
        if self._connection is None:
            return None
        if self._cursor_closed:
            raise errors.Error(ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED,
                get_error_message(self._connection._locale, ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED))
        if not self.query_executed:
            raise errors.Error(ERROR_ID.ERROR_NO_ACTIVE_QUERY,
                get_error_message(self._connection._locale, ERROR_ID.ERROR_NO_ACTIVE_QUERY))
        if self._query_results is None:
            return None

        _Fetch = None
        try:
            while len(self._query_results) < self._total_rows_count:
                _Fetch = Fetch(self._connection._socket,
                    self._connection._CAS_INFO,
                    self.query_handle,
                    self._query_packet,
                    self._rows_count,
                    self._connection._locale)

                _Fetch.SendRequest()
                _Fetch.GetResponse()
                self._query_results += _Fetch._fetch_results
                self._rows_count += len(_Fetch._fetch_results)

            self._query_cursor_pos = 0 # reset cursor position at the beginning

            return self._query_results
        except IndexError:
            return None
        except Exception:
            self._set_error(_Fetch.error_code, _Fetch.error_msg)
            raise

    def nextset(self):
        """
        (This method is optional since not all databases support multiple result sets. [3])
        This method will make the cursor skip to the next
        available set, discarding any remaining rows from the current set.
        """
        raise errors.Error(ERROR_ID.ERROR_NOT_IMPLEMENTED,
            get_error_message(self._connection._locale, ERROR_ID.ERROR_NOT_IMPLEMENTED))

    def arraysize(self, size):
        """
        This read/write attribute specifies the number of rows to
        fetch at a time with .fetchmany(). It defaults to 1
        meaning to fetch a single row at a time.
        """
        if self._connection is None:
            return None
        if self._cursor_closed:
            raise errors.Error(ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED,
                get_error_message(self._connection._locale, ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED))
        if size is not None:
            if size > 0:
                self._array_size = size

    def setinputsizes(self, sizes):
        """
        This can be used before a call to .execute*() to
        predefine memory areas for the operation's parameters.

        sizes is specified as a sequence -- one item for each
        input parameter.  The item should be a Type Object that
        corresponds to the input that will be used, or it should
        be an integer specifying the maximum length of a string
        parameter.  If the item is None, then no predefined memory
        area will be reserved for that column (this is useful to
        avoid predefined areas for large inputs).
        """
        # We will not implement this in the first release
        if self._connection is None:
            return None
        if self._cursor_closed:
            raise errors.Error(ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED,
                get_error_message(self._connection._locale, ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED))

        pass

    def setoutputsize(self, size, column = None):
        """
        Set a column buffer size for fetches of large columns
        (e.g. LONGs, BLOBs, etc.).  The column is specified as an
        index into the result sequence.  Not specifying the column
        will set the default size for all large columns in the cursor.

        Implementations are free to have this method do nothing
        and users are free to not use it.
        """
        # We will not implement this in the first release
        if self._connection is None:
            return None
        if self._cursor_closed:
            raise errors.Error(ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED,
                get_error_message(self._connection._locale, ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED))

        pass

    def next(self):
        """
        Used for iterating over the result set.
        Calls self.fetchone() to get the next row.
        """
        try:
            row = self.fetchone()
        except errors.InterfaceError:
            raise StopIteration
        if not row:
            raise StopIteration
        return row

    @property
    def rownumber(self):
        """
        Row number information
        """
        if self._connection is None:
            return None
        if self._cursor_closed:
            raise errors.Error(ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED,
                get_error_message(self._connection._locale, ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED))
        if self._query_cursor_pos >= 1:
            return self._query_cursor_pos - 1
        else:
            return -1

    @property
    def column_names(self):
        """
        Returns column names
        This property returns the columns names as a tuple.
        Returns a tuple.
        """
        if self._connection is None:
            return None
        if self._cursor_closed:
            raise errors.Error(ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED,
                get_error_message(self._connection._locale, ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED))
        if not self.description:
            return None

        return tuple([d[0] for d in self.description])

    @property
    def last_query(self):
        """
        Returns last executed query
        """
        return self._last_query

    @property
    def connection(self):
        """
        Returns cursor connection
        """
        if self._connection is None:
            return None

        return self._connection

    def num_rows(self):
        if self._connection is None:
            return None
        if self._cursor_closed:
            raise errors.Error(ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED,
                get_error_message(self._connection._locale, ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED))

        return self._rows_count
