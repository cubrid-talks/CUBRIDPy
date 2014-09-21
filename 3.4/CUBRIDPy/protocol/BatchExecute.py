from CUBRIDPy.constants.DATA_TYPES import *
from CUBRIDPy.constants.CUBRID import *
from CUBRIDPy.protocol.CUBRIDProtocol import CUBRIDProtocol
import logging

class BatchExecute(CUBRIDProtocol):
    """
    BatchExecute class implementation
    """

    def __init__(self, sock, CAS_INFO, SQLs, auto_commit, locale = 'en-US'):
        """
        Class constructor
        :param socket sock: Connection socket
        :param array CAS_INFO: CAS
        :param array SQLs: SQL statements to execute in batch mode
        :param bool auto_commit: Auto-Commit mode
        :param str locale: Locale
        :return:
        """
        logging.debug('Initializing BatchExecute...')
        assert isinstance(SQLs, (list, tuple))
        self._SQLs = SQLs
        self._auto_commit = 1 if auto_commit else 0

        self.results_error_codes = []
        self.results_error_msgs = []

        super(BatchExecute, self).__init__(sock, CAS_INFO, locale)


    def SendRequest(self):
        """
        Send request to the database engine
        """
        logging.debug('Send batch execute request...')
        # specific data to be sent (array of: [part_length, part])
        data = [
            # command code
            [DATA_TYPES.BYTE_SIZEOF, CAS_FUNCTION_CODE.CAS_FC_EXECUTE_BATCH],
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.BYTE_SIZEOF],
            # data
            [DATA_TYPES.BYTE_SIZEOF, self._auto_commit]
        ]
        packet_length = sum(row[0] for row in data)
        # add SQLs
        for i in xrange(0, len(self._SQLs)):
            data.append([DATA_TYPES.INT_SIZEOF, len(self._SQLs[i]) + 1])
            data.append([DATA_TYPES.VARIABLE_STRING_SIZEOF, self._SQLs[i]])
            data.append([DATA_TYPES.BYTE_SIZEOF, 0]) # null-terminator
            # adjust packet length with string variable size
            packet_length += DATA_TYPES.INT_SIZEOF + len(self._SQLs[i]) + DATA_TYPES.BYTE_SIZEOF

        super(BatchExecute, self).PrepareRequestData(data, packet_length)
        super(BatchExecute, self).SendRequest()


    def ProcessResponse(self):
        """
        Process response
        """
        logging.debug('Batch execute data received.')
        self.executed_count = self._response_buffer.ReadInt()
        for i in xrange(0, self.executed_count):
            self._response_buffer.ReadByte() # not used
            result = self._response_buffer.ReadInt()
            if result < 0:
                err_msg_length = self._response_buffer.ReadInt()
                err_msg = self._response_buffer.ReadNullTerminatedString(err_msg_length)
                self.results_error_codes.append(result)
                self.results_error_msgs.append(err_msg)
                logging.debug(self._SQLs[i] + '\nreturned error:\n' + err_msg)
            else:
                self.results_error_codes.append(result)
                self.results_error_msgs.append('') # no error message
                self._response_buffer.ReadInt() # not used
                self._response_buffer.ReadShort() # not used
                self._response_buffer.ReadShort() # not used


    def GetResponse(self):
        """
        Get response from the database engine
        """
        logging.debug('Reading database engine version response...')
        super(BatchExecute, self).GetResponse()

