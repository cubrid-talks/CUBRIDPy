from CUBRIDPy.errors import *
from CUBRIDPy.constants.DATA_TYPES import *
from CUBRIDPy.constants.CUBRID import *
from CUBRIDPy.localization.ERROR_ID import ERROR_ID
from CUBRIDPy.localization.localization import get_error_message
from CUBRIDPy.protocol.CUBRIDProtocol import CUBRIDProtocol
import logging

class LOBRead(CUBRIDProtocol):
    """
    LOBRead class implementation
    """

    def __init__(self, sock, CAS_INFO, lob_handle, lob_type, read_position, length_to_read, locale = 'en-US'):
        """
        Class constructor
        :param socket sock: Connection socket.
        :param array CAS_INFO: CAS data.
        :param str locale: localization information
        :return:
        """
        logging.debug('Initializing LOB Read...')
        self._lob_handle = lob_handle
        self._lob_type = lob_type
        self._read_position = read_position
        self._length_to_read = length_to_read

        self.lob_buffer = None

        super(LOBRead, self).__init__(sock, CAS_INFO, locale)

        return

    def SendRequest(self):
        """
        Send request to the database engine
        """
        logging.debug('Sending LOB read request...')

        # specific data to be sent (array of: [part_length, part])
        data = [
            # command code
            [DATA_TYPES.BYTE_SIZEOF, CAS_FUNCTION_CODE.CAS_FC_LOB_READ],
            # LOB handle data length
            [DATA_TYPES.INT_SIZEOF, len(self._lob_handle)], # LOB handle size
            # LOB handle data
            [DATA_TYPES.UNSPECIFIED_SIZEOF, self._lob_handle], # LOB
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.LONG_SIZEOF],
            # data
            [DATA_TYPES.LONG_SIZEOF, self._read_position], # start position from which to read data
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.INT_SIZEOF],
            # data
            [DATA_TYPES.INT_SIZEOF, self._length_to_read] # number of bytes to read
        ]

        packet_length = sum(row[0] for row in data)
        # adjust packet length with LOB handle variable size
        packet_length += len(self._lob_handle)

        super(LOBRead, self).PrepareRequestData(data, packet_length)
        super(LOBRead, self).SendRequest()

        return

    def ProcessResponse(self):
        """
        Process response
        """
        logging.debug('LOB read data received successfully.')

        if self._lob_type == CUBRID_DATA_TYPE.CCI_U_TYPE_BLOB:
            self.lob_buffer = self._response_buffer.ReadBytesArray(self.response_code)
        elif self._lob_type == CUBRID_DATA_TYPE.CCI_U_TYPE_CLOB:
            self.lob_buffer = self._response_buffer.ReadString(self.response_code)
        else:
            raise Error(get_error_message(self._locale, ERROR_ID.ERROR_INVALID_LOB_TYPE))

        return

    def GetResponse(self):
        """
        Get response from the database engine
        """
        logging.debug('Reading LOB read response...')
        super(LOBRead, self).GetResponse()

        return
