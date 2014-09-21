from CUBRIDPy.constants.DATA_TYPES import *
from CUBRIDPy.constants.CUBRID import *
from CUBRIDPy.localization.ERROR_ID import ERROR_ID
from CUBRIDPy.localization.localization import *
from CUBRIDPy.protocol.ResponseBuffer import ResponseBuffer
import CUBRIDPy.utils.helpers as helpers
import logging

class CUBRIDProtocol(object):
    """
    CUBRID Protocol class implementation
    """

    def __init__(self, sock, CAS_INFO, locale = 'en-US'):
        """
        Class constructor
        :param socket sock: Connection socket.
        :param array CAS_INFO: CAS
        :param str locale: Locale
        """
        self._sock = sock
        self._CAS_INFO = CAS_INFO
        # Error information
        self.response_code = 0
        self.error_code = 0
        self.error_msg = ''
        # Locale
        self._locale = locale

        self._data = None
        self._response = None
        self._read_position = 0
        self._packet_length = None
        self._response_buffer = None

    def PrepareRequestData(self, data, packet_length = None):
        """
        Prepare request data
        :param array data: Data to be sent
        :param int packet_length: Packet length
        """
        self._data = data

        # length of specific data to be sent
        if packet_length is None:
            # calculate default length
            self._packet_length = sum(row[0] for row in self._data)
        else:
            self._packet_length = packet_length

    def SendRequest(self):
        """
        Send request to the database engine
        """
        send_buffer = [
            # fixed part
            helpers.int_to_send_buffer(self._packet_length),
            ''.join(chr(x) for x in self._CAS_INFO),
            # variable part
            self._prepare_send_buffer(self._data)
        ]

        try:
            prepared_send_buffer = ''.join(send_buffer)
            self._sock.sendall(prepared_send_buffer)
        except:
            raise Exception(get_error_message(self._locale, ERROR_ID.ERROR_SENDING_DATA))

    def ProcessResponse(self):
        """
        Process response
        To be overwritten in child classes
        """
        raise Exception('This method should always be overwritten in the child classes!')

    def ProcessResponseFetch(self):
        """
        Process response
        To be overwritten in child classes
        """
        raise Exception('This method should always be overwritten in the child classes!')

    def GetResponse(self):
        """
        Get response from the database engine
        """
        try:
            self._read_response()
            if self.response_code < 0:
                self._read_error_info()
                raise Exception('%d: %s' % (self.error_code, self.error_msg))
            else:
                self.ProcessResponse()
        except Exception as ex:
            logging.error(str(ex)) # log original error
            raise Exception(get_error_message(self._locale, ERROR_ID.ERROR_RECEIVING_DATA))

    def GetResponseFetch(self):
        """
        Get response from the database engine
        """
        try:
            self._read_response()

            if self.response_code < 0:
                self._read_error_info()
                raise Exception('%d: %s' % (self.error_code, self.error_msg))
            else:
                self.ProcessResponseFetch()
        except Exception as ex:
            logging.error(str(ex)) # log original error
            raise Exception(get_error_message(self._locale, ERROR_ID.ERROR_RECEIVING_DATA))

    def _read_response(self):
        """
        Read the socket data response and parse the fixed part of the protocol
        """
        # Parse header response data
        expectedResponseLength = SOCKET.CAS_INFO_SIZEOF + helpers.chr_array_to_num(
            self._sock.recv(SOCKET.HEADER_RESPONSE_SIZEOF),
            0,
            DATA_TYPES.INT_SIZEOF)
        # Continue reading response data
        total_data = []
        total_len = 0
        while total_len < expectedResponseLength:
            total_data.append(self._sock.recv(expectedResponseLength))
            total_len = sum([len(i) for i in total_data])

        self._response = ''.join(total_data)
        self._response_buffer = ResponseBuffer(self._response)
        self._CAS_INFO = self._response_buffer.ReadBytesArray(SOCKET.CAS_INFO_SIZEOF)
        self.response_code = self._response_buffer.ReadInt()

    def _read_error_info(self):
        """
        Parse error information
        """
        self.error_code = self._response_buffer.ReadInt()
        self.error_msg = self._response_buffer.ReadStringToEnd()

    def _prepare_send_buffer(self, data):
        """
        Prepares data to be sent in the CUBRID protocol format
        :param data: Data to be sent
        :return: Data to be sent in the CUBRID protocol format
        """
        send_buffer = []

        for i in range(0, len(data)):
            if data[i][0] == DATA_TYPES.BYTE_SIZEOF:
                send_buffer.append(helpers.byte_to_send_buffer(data[i][1]))
                continue
            if data[i][0] == DATA_TYPES.INT_SIZEOF:
                send_buffer.append(helpers.int_to_send_buffer(data[i][1]))
                continue
            if data[i][0] == DATA_TYPES.LONG_SIZEOF:
                send_buffer.append(helpers.long_to_send_buffer(data[i][1]))
                continue
            if data[i][0] == DATA_TYPES.VARIABLE_STRING_SIZEOF:
                send_buffer.append(data[i][1])
                continue
            if data[i][0] == DATA_TYPES.UNSPECIFIED_SIZEOF:
                lob_data = data[i][1]
                send_buffer.append(helpers.get_string_from_bytes_array(lob_data, 0, len(lob_data)))
                continue

            # do not accept other types
            raise Exception('Unexpected data type!')

        return ''.join(send_buffer)

