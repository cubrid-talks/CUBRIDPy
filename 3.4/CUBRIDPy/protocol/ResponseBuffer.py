from CUBRIDPy.constants.CUBRID import SOCKET
from CUBRIDPy.constants.DATA_TYPES import *
from CUBRIDPy.utils import helpers

class ResponseBuffer(object):
    """
    ResponseBuffer class implementation
    """

    def __init__(self, buffer, read_position = 0):
        """
        Class constructor
        :param str buffer: buffer data.
        :param int read_position: read_position.
        """
        self._buffer = buffer
        self._read_position = read_position

        return

    def ResponseBufferLength(self):
        """
        Return total response buffer length
        :return:
        """
        return len(self._buffer)

    def VariablePartResponseBufferLength(self):
        """
        Return variable part response buffer length
        :return:
        """
        return len(self._buffer) - SOCKET.CAS_INFO_SIZEOF - DATA_TYPES.INT_SIZEOF

    def ReadBytesArray(self, size):
        val = helpers.get_bytes_array_from_string(self._buffer, self._read_position, size)
        self._read_position += size

        if self._read_position > self.ResponseBufferLength():
            raise Exception('Invalid read position!')

        return val

    def ReadByte(self):
        val = helpers.chr_array_to_num(self._buffer, self._read_position, DATA_TYPES.BYTE_SIZEOF)
        self._read_position += DATA_TYPES.BYTE_SIZEOF

        if self._read_position > self.ResponseBufferLength():
            raise Exception('Invalid read position!')

        return val

    def ReadShort(self):
        val = helpers.chr_array_to_num(self._buffer, self._read_position, DATA_TYPES.SHORT_SIZEOF)
        self._read_position += DATA_TYPES.SHORT_SIZEOF

        if self._read_position > self.ResponseBufferLength():
            raise Exception('Invalid read position!')

        return val

    def ReadInt(self):
        val = helpers.signed_int(helpers.chr_array_to_num(self._buffer, self._read_position, DATA_TYPES.INT_SIZEOF))
        self._read_position += DATA_TYPES.INT_SIZEOF

        if self._read_position > self.ResponseBufferLength():
            raise Exception('Invalid read position!')

        return val

    def ReadLong(self):
        val = helpers.signed_long(helpers.chr_array_to_num(self._buffer, self._read_position, DATA_TYPES.LONG_SIZEOF))
        self._read_position += DATA_TYPES.LONG_SIZEOF

        if self._read_position > self.ResponseBufferLength():
            raise Exception('Invalid read position!')

        return val

    def ReadString(self, size):
        val = self._buffer[self._read_position:self._read_position + size]
        self._read_position += size

        if self._read_position > self.ResponseBufferLength():
            raise Exception('Invalid read position!')

        return val

    def ReadNullTerminatedString(self, size):
        val = self.ReadString(size - 1)
        self.ReadStringNullTerminator() # read null-terminator

        if self._read_position > self.ResponseBufferLength():
            raise Exception('Invalid read position!')

        return val

    def ReadStringToEnd(self):
        val = self._buffer[self._read_position:-1]
        self._read_position = self.ResponseBufferLength()

        if self._read_position > self.ResponseBufferLength():
            raise Exception('Invalid read position!')

        return val

    def ReadStringNullTerminator(self):
        val = self.ReadByte()

        if self._read_position > self.ResponseBufferLength():
            raise Exception('Invalid read position!')

        if val != 0:
            raise Exception('Invalid null-terminated string!')

        return val

    def ReadFloat(self):
        val = helpers.chr_array_to_float(self._buffer, self._read_position, DATA_TYPES.FLOAT_SIZEOF)
        self._read_position += DATA_TYPES.FLOAT_SIZEOF

        if self._read_position > self.ResponseBufferLength():
            raise Exception('Invalid read position!')

        return val

    def ReadDouble(self):
        val = helpers.chr_array_to_double(self._buffer, self._read_position, DATA_TYPES.DOUBLE_SIZEOF)
        self._read_position += DATA_TYPES.DOUBLE_SIZEOF

        if self._read_position > self.ResponseBufferLength():
            raise Exception('Invalid read position!')

        return val

    def ReadNumeric(self, size):
        val = float(self._buffer[self._read_position: self._read_position + size - 1])
        self._read_position += size

        if self._read_position > self.ResponseBufferLength():
            raise Exception('Invalid read position!')

        return val

    def ReadDate(self):
        val = helpers.chr_array_to_date(self._buffer, self._read_position)
        self._read_position += DATA_TYPES.DATE_SIZEOF

        if self._read_position > self.ResponseBufferLength():
            raise Exception('Invalid read position!')

        return val

    def ReadTime(self):
        val = helpers.chr_array_to_time(self._buffer, self._read_position)
        self._read_position += DATA_TYPES.TIME_SIZEOF

        if self._read_position > self.ResponseBufferLength():
            raise Exception('Invalid read position!')

        return val

    def ReadDateTime(self):
        val = helpers.chr_array_to_datetime(self._buffer, self._read_position)
        self._read_position += DATA_TYPES.DATETIME_SIZEOF

        if self._read_position > self.ResponseBufferLength():
            raise Exception('Invalid read position!')

        return val

    def ReadTimeStamp(self):
        val = helpers.chr_array_to_timestamp(self._buffer, self._read_position)
        self._read_position += DATA_TYPES.TIMESTAMP_SIZEOF

        if self._read_position > self.ResponseBufferLength():
            raise Exception('Invalid read position!')

        return val

    def ReadOID(self):
        val = helpers.chr_array_to_num(self._buffer, self._read_position, DATA_TYPES.OID_SIZEOF)
        self._read_position += DATA_TYPES.OID_SIZEOF

        if self._read_position > self.ResponseBufferLength():
            raise Exception('Invalid read position!')

        return val

