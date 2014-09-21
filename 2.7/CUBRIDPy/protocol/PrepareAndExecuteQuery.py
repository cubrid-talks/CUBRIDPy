from CUBRIDPy.constants.DATA_TYPES import *
from CUBRIDPy.constants.CUBRID import *
from CUBRIDPy.protocol.CUBRIDProtocol import CUBRIDProtocol
from CUBRIDPy.protocol.ColumnMetaData import ColumnMetaData
from CUBRIDPy.protocol.ResultInfo import ResultInfo
import datetime
import logging
from array import array
from CUBRIDPy.utils.helpers import *

class PrepareAndExecuteQuery(CUBRIDProtocol):
    """
    PrepareAndExecuteQuery class implementation
    """

    def __init__(self, sock, CAS_INFO, SQL, auto_commit_mode, locale = 'en-US'):
        """
         Class constructor
        :type CAS_INFO: array
        :param socket sock: Connection socket.
        :param array CAS_INFO: CAS data.
        :param SQL: SQL statement to execute
        :param auto_commit_mode: Auto-Commit mode
        :param locale: locale data
        :return:
        """
        logging.debug('Initializing PrepareAndExecuteQuery...')
        self._SQL = SQL
        self._autoCommit = 1 if auto_commit_mode else 0
        # Result information
        self._read_length = None
        self._total_tuple_count = 0  # Total number of tuples
        self._result_cache_lifetime = 0  # Cache lifetime
        self._statement_type = 0  # Statement type
        self._bind_count = 0  # Bind count
        self._is_updatable = False  # Is updatable
        self._cache_reusable = 0  # Cache reusable
        self._result_count = 0  # Number of results
        self._column_count = 0  # Number of columns
        self._info_array = []  # Column meta data
        self._result_infos = []  # Result info
        self._current_tuple_count = 0  # Current number of returned tuples
        self._tuple_count = 0  # Number of tuples
        self._fetch_code = 0  # Result fetch code

        super(PrepareAndExecuteQuery, self).__init__(sock, CAS_INFO, locale)

        return

    def SendRequest(self):
        """
        Send request to the database engine
        """
        logging.debug('Sending query request...')

        # specific data to be sent (array of: [part_length, part])
        data = [
            [DATA_TYPES.BYTE_SIZEOF, CAS_FUNCTION_CODE.CAS_FC_PREPARE_AND_EXECUTE],
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.INT_SIZEOF],
            [DATA_TYPES.INT_SIZEOF, 3],
            # SQL
            [DATA_TYPES.INT_SIZEOF, len(self._SQL) + 1],
            [DATA_TYPES.VARIABLE_STRING_SIZEOF, self._SQL],
            [DATA_TYPES.BYTE_SIZEOF, 0],
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.BYTE_SIZEOF],
            # data
            [DATA_TYPES.BYTE_SIZEOF, CCI_PREPARE_OPTION.CCI_PREPARE_NORMAL], # Prepare option
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.BYTE_SIZEOF],
            # data
            [DATA_TYPES.BYTE_SIZEOF, self._autoCommit], # Autocommit mode
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.BYTE_SIZEOF],
            # data
            [DATA_TYPES.BYTE_SIZEOF, CCI_EXECUTION_OPTION.CCI_EXEC_QUERY_ALL], # Execute option
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.INT_SIZEOF],
            # data
            [DATA_TYPES.INT_SIZEOF, 0], # Reserved (Max. col. size)
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.INT_SIZEOF],
            # data
            [DATA_TYPES.INT_SIZEOF, 0], # Reserved (Max, row size)
            # data length
            [DATA_TYPES.INT_SIZEOF, 0], # Reserved null
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.LONG_SIZEOF],
            # data
            [DATA_TYPES.INT_SIZEOF, 0], # Cache time - seconds
            # data
            [DATA_TYPES.INT_SIZEOF, 0], # Cache time - milliseconds
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.INT_SIZEOF],
            # data
            [DATA_TYPES.INT_SIZEOF, 0] # Query timeout
        ]

        packet_length = sum(row[0] for row in data)
        packet_length += 1 # revert VARIABLE_STRING_SIZEOF value
        packet_length += len(self._SQL) # add SQL length

        super(PrepareAndExecuteQuery, self).PrepareRequestData(data, packet_length)
        super(PrepareAndExecuteQuery, self).SendRequest()

        return

    def ProcessResponse(self):
        """
        Process response
        """
        self.query_handle = self.response_code
        self._result_cache_lifetime = self._response_buffer.ReadInt()
        self._statement_type = self._response_buffer.ReadByte()
        self._bind_count = self._response_buffer.ReadInt()
        self._is_updatable = self._response_buffer.ReadByte()
        self._column_count = self._response_buffer.ReadInt()

        # Read column info
        for i in range(0, self._column_count):
            info = ColumnMetaData()
            info.column_type = self._response_buffer.ReadByte()
            info.scale = self._response_buffer.ReadShort()
            info.precision = self._response_buffer.ReadInt()
            length = self._response_buffer.ReadInt()
            info.name = self._response_buffer.ReadNullTerminatedString(length)
            length = self._response_buffer.ReadInt()
            info.real_name = self._response_buffer.ReadNullTerminatedString(length)
            length = self._response_buffer.ReadInt()
            info.table_name = self._response_buffer.ReadNullTerminatedString(length)
            info.is_nullable = (self._response_buffer.ReadByte() == 1)
            length = self._response_buffer.ReadInt()
            info.default_value = self._response_buffer.ReadNullTerminatedString(length)
            info.is_auto_increment = (self._response_buffer.ReadByte() == 1)
            info.is_unique_key = (self._response_buffer.ReadByte() == 1)
            info.is_primary_key = (self._response_buffer.ReadByte() == 1)
            info.is_reverse_index = (self._response_buffer.ReadByte() == 1)
            info.is_reverse_unique = (self._response_buffer.ReadByte() == 1)
            info.is_foreign_key = (self._response_buffer.ReadByte() == 1)
            info.is_shared = (self._response_buffer.ReadByte() == 1)
            self._info_array.append(info)

        self._total_tuple_count = self._response_buffer.ReadInt()
        self._cache_reusable = self._response_buffer.ReadByte()
        self._result_count = self._response_buffer.ReadInt()

        # Read Result info
        for i in range(0, self._result_count):
            resultInfo = ResultInfo()
            resultInfo.stmt_type = self._response_buffer.ReadByte()
            resultInfo.result_count = self._response_buffer.ReadInt()
            resultInfo.oid = self._response_buffer.ReadOID()
            resultInfo.cache_time_sec = self._response_buffer.ReadInt()
            resultInfo.cache_time_usec = self._response_buffer.ReadInt()
            self._result_infos.append(resultInfo)

        if self._statement_type == STATEMENT_TYPE.CUBRID_STMT_SELECT:
            self._fetch_code = self._response_buffer.ReadInt()
            self._tuple_count = self._response_buffer.ReadInt()
            self._get_columns_data(self._tuple_count, self._response_buffer)
            self.description = []
            for i in range(0, self._column_count):
                # A Cursor Object's description attribute returns information about each of the result columns of
                # a query.
                # The type_code must compare equal to one of Type Objects defined below:
                # (http://www.python.org/dev/peps/pep-0249/#date)
                # Date(year, month, day)
                # Time(hour, minute, second)
                # Timestamp(year, month, day, hour, minute, second)
                # DateFromTicks(ticks)
                # TimeFromTicks(ticks)
                # TimestampFromTicks(ticks)
                # Binary(string)
                # STRING type
                # BINARY type
                # NUMBER type
                # DATETIME type
                # ROWID type
                # SQL NULL values are represented by the Python None singleton on input and output.
                type_code = None # Default value
                if self._info_array[i].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_BIGINT:
                    type_code = type(long)
                elif  self._info_array[i].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_BIT:
                    type_code = type(bool)
                elif  self._info_array[i].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_BLOB:
                    type_code = type(object)
                elif  self._info_array[i].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_CHAR:
                    type_code = type(str)
                elif  self._info_array[i].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_CLOB:
                    type_code = type(object)
                elif  self._info_array[i].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_DATE:
                    type_code = type(datetime.date)
                elif  self._info_array[i].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_DATETIME:
                    type_code = type(datetime.datetime)
                elif  self._info_array[i].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_DOUBLE:
                    type_code = type(float)
                elif  self._info_array[i].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_FLOAT:
                    type_code = type(float)
                elif  self._info_array[i].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_INT:
                    type_code = type(int)
                elif  self._info_array[i].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_MONETARY:
                    type_code = type(float)
                elif  self._info_array[i].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_NCHAR:
                    type_code = type(str)
                elif  self._info_array[i].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_NUMERIC:
                    type_code = type(float)
                elif  self._info_array[i].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_OBJECT:
                    type_code = type(object)
                elif  self._info_array[i].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_SHORT:
                    type_code = type(int)
                elif  self._info_array[i].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_SMALLINT:
                    type_code = type(int)
                elif  self._info_array[i].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_STRING:
                    type_code = type(str)
                elif  self._info_array[i].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_TIME:
                    type_code = type(datetime.time)
                elif  self._info_array[i].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_TIMESTAMP:
                    type_code = type(datetime.time)
                elif  self._info_array[i].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_VARCHAR:
                    type_code = type(str)
                elif  self._info_array[i].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_VARNCHAR:
                    type_code = type(str)

                column_description = (
                    self._info_array[i].name,
                    type_code,
                    None, #display_size
                    None, #internal_size
                    self._info_array[i].precision,
                    self._info_array[i].scale,
                    self._info_array[i].is_nullable
                    )
                self.description.append(column_description)

        return

    def GetResponse(self):
        """
        Get response from the database engine
        """
        logging.debug('Reading prepare and execute response...')
        super(PrepareAndExecuteQuery, self).GetResponse()

        return

    def _get_columns_data(self, tuple_count, response_buffer):
        #noinspection PyUnusedLocal
        columnValues = [[None for x in xrange(self._column_count)] for x in xrange(tuple_count)]
        for i in range(0, tuple_count):
            response_buffer.ReadInt() # index
            response_buffer.ReadOID() # oid
            for j in range(0, self._column_count):
                size = response_buffer.ReadInt()
                if size <= 0:
                    columnValues[i][j] = None
                else:
                    if (self._statement_type == STATEMENT_TYPE.CUBRID_STMT_CALL or
                        self._statement_type == STATEMENT_TYPE.CUBRID_STMT_EVALUATE or
                        self._statement_type == STATEMENT_TYPE.CUBRID_STMT_CALL_SP or
                        self._info_array[j].column_type == CUBRID_DATA_TYPE.CCI_U_TYPE_NULL):
                        data_type = response_buffer.ReadByte()
                        size -= 1
                    else:
                        data_type = self._info_array[j].column_type

                    read_value = self._read_column_value(data_type, size, response_buffer)
                    columnValues[i][j] = read_value

        self.results = columnValues

        return columnValues

    def _read_column_value(self, data_type, size, response_buffer):
        if (data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_CHAR or
            data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_NCHAR or
            data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_STRING or
            data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_VARNCHAR):
            return response_buffer.ReadNullTerminatedString(size)
        elif data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_SHORT:
            return response_buffer.ReadShort()
        elif data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_INT:
            return response_buffer.ReadInt()
        elif data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_BIGINT:
            return response_buffer.ReadLong()
        elif data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_FLOAT:
            return response_buffer.ReadFloat()
        elif (data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_DOUBLE or
              data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_MONETARY):
            return response_buffer.ReadDouble()
        elif data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_NUMERIC:
            return response_buffer.ReadNumeric(size)
        elif data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_DATE:
            return response_buffer.ReadDate()
        elif data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_TIME:
            return response_buffer.ReadTime()
        elif data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_DATETIME:
            return response_buffer.ReadDateTime()
        elif data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_TIMESTAMP:
            return response_buffer.ReadTimeStamp()
        elif data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_OBJECT:
            # TODO Verify the size to read below
            response_buffer.ReadInt()
            response_buffer.ReadShort()
            response_buffer.ReadShort()

            return 'Not supported yet!'
        elif (data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_BIT or
              data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_VARBIT):
            # TODO Verify the size to read below
            response_buffer.ReadByte()

            return 'Not supported yet!'
        elif (data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_SET or
              data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_MULTISET or
              data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_SEQUENCE):
            count = response_buffer.ReadInt()
            size = response_buffer.ReadInt()
            for i in range(1, size * count):
                response_buffer.ReadByte()

            return 'Not supported yet!'
        elif data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_BLOB:
            lob_handle = response_buffer.ReadBytesArray(size)
            response = lob_handle[DATA_TYPES.INT_SIZEOF:DATA_TYPES.INT_SIZEOF + DATA_TYPES.LONG_SIZEOF]
            lob_buffer_size = bytes_array_to_num(response, 0, len(response))

            file_locator = ''
            for i in xrange(16, len(lob_handle) - 1):
                file_locator += str(unichr(lob_handle[i]))
            logging.debug('File locator: ' + file_locator)

            return lob_handle, CUBRID_DATA_TYPE.CCI_U_TYPE_BLOB, lob_buffer_size
        elif data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_CLOB:
            lob_handle = response_buffer.ReadBytesArray(size)
            response = lob_handle[DATA_TYPES.INT_SIZEOF:DATA_TYPES.INT_SIZEOF + DATA_TYPES.LONG_SIZEOF]
            lob_buffer_size = bytes_array_to_num(response, 0, len(response))

            file_locator = ''
            for i in xrange(16, len(lob_handle) - 1):
                file_locator += str(unichr(lob_handle[i]))
            logging.debug('File locator: ' + file_locator)

            return lob_handle, CUBRID_DATA_TYPE.CCI_U_TYPE_CLOB, lob_buffer_size
        elif data_type == CUBRID_DATA_TYPE.CCI_U_TYPE_RESULTSET:
            return 'Not supported yet!'
        else:
            return None

        # TODO finish implementing this method

