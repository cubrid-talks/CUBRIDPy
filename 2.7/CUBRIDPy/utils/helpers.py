"""
Various driver utility functions
"""
import re
import struct
import math
import datetime
from struct import unpack

def escape(str):
    """
    Escape a string.
    :param string str: String to escape.
    """

    return re.escape(str)


def read_bytes(buf, bytes_count):
    """
    Reads [bytes_count] bytes from a buffer.
    :param buffer buf: Source buffer.
    :param int bytes_count: How many bytes to read.
    """
    return buf[0:bytes_count]

def buffer_to_int(buf, start_pos, length):
    """
    Unpacks the buffer to an integer-type value
    :param buffer buf: Source buffer.
    :param int start_pos: Buffer content start position.
    :param int length: Buffer content size to be converted to int-value.
    """
    my_buf = buf[start_pos:start_pos + length]
    try:
        if length == 1:
            return int(ord(my_buf[0]))
        if length <= 4:
            tmp = my_buf + '\x00' * (4 - length)
            return struct.unpack('<I', tmp)[0]
        else:
            tmp = my_buf + '\x00' * (8 - length)
            return struct.unpack('<Q', tmp)[0]
    except:
        raise

def get_bytes_array_from_string(source, source_start_pos, source_bytes_count):
    """
    Returns a bytes array from a string.
    :param string source: Source string
    :param int source_start_pos: Start position in the string
    :param int source_bytes_count: How many string chars to use for the bytes array
    :return array: A bytes array
    """
    value = []
    for i in range(source_start_pos, source_start_pos + source_bytes_count):
        value.append(ord(source[i]))

    return value

def get_string_from_bytes_array(source, source_start_pos, source_bytes_count):
    """
    Returns a string from a bytes array.
    :param array source: Source bytes array
    :param int source_start_pos: Start position in the bytes array
    :param int source_bytes_count: How many bytes to use for the string
    :return array: A string
    """
    return ''.join(chr(x) for x in source[source_start_pos:source_bytes_count])

def verify_strict_positive(num):
    """
    Verify if a number is strict positive.
    :param int num: Number to verify.
    """
    if num > 0:
        return True
    else:
        return False

def verify_string_min_len(str, length):
    """
    Verify if a string has a minimum length.
    :param string str: String to verify.
    :param int length: Minimum length.
    """
    if len(str.strip()) >= length:
        return True
    else:
        return False

def bytes_array_to_num(source_array, start_pos, bytes_length):
    """
    Converts a bytes array to a int-type number.
    :param array source_array: Source bytes array.
    :param int start_pos: Source start position.
    :param int bytes_length: How many consecutive bytes to use for conversion.
    """
    val = 0
    for i in range(start_pos, start_pos + bytes_length):
        power = bytes_length - (i - start_pos) - 1
        val += source_array[i] * int(math.pow(256, power))

    return val

def chr_array_to_num(chr_array, start_pos, bytes_length):
    """
    Converts a char array to a int-type number.
    :param array chr_array: Source char array.
    :param int start_pos: Source start position.
    :param int bytes_length: How many consecutive bytes to use for conversion.
    """
    return bytes_array_to_num(bytearray(chr_array), start_pos, bytes_length)

def num_to_bytes_array(num, bytes_length):
    """
    Converts a int-type number to a bytes array.
    :param int num: Source number.
    :param int bytes_length: How many consecutive bytes to use for conversion.
    """
    bytes_arr = []
    for i in range(0, bytes_length):
        pos = 8 * (bytes_length - i - 1 )
        val = (num >> pos) & 0xFF
        bytes_arr.append(val)

    return bytes_arr

def num_to_chr_array(num, bytes_length):
    """
    Converts a int-type number to a char array.
    :param int num: Source number.
    :param int bytes_length: How many consecutive bytes to use for conversion.
    """
    bytes_arr = num_to_bytes_array(num, bytes_length)

    return ''.join(chr(x) for x in bytes_arr)

def byte_to_send_buffer(num):
    """
    Converts a byte-type number to a string to be sent to the engine.
    :param int num: Source byte.
    """
    chr_array = num_to_chr_array(num, 1)

    return '%c' % (chr_array[0])

def int_to_send_buffer(num):
    """
    Converts an int-type number to a string to be sent to the engine.
    :param int num: Source integer.
    """
    chr_array = num_to_chr_array(num, 4)

    return ''.join(x for x in chr_array)

def long_to_send_buffer(num):
    """
    Converts a long-type number to a string to be sent to the engine.
    :param int num: Source integer.
    """
    chr_array = num_to_chr_array(num, 8)

    return chr_array

def signed_short(num):
    """
    Converts an unsigned short value to a signed value
    :param short num: Number to convert
    :return: A signed value
    """
    if num < 0:
        return num
    else:
        mask = (2 ** 16) - 1
        if num & (1 << (16 - 1)):
            return num | ~mask
        else:
            return num & mask

def signed_int(num):
    """
    Converts an unsigned int value to a signed value
    :param int num: Number to convert
    :return: A signed value
    """
    if num < 0:
        return num
    else:
        mask = (2 ** 32) - 1
        if num & (1 << (32 - 1)):
            return num | ~mask
        else:
            return num & mask

def signed_long(num):
    """
    Converts an unsigned long value to a signed value
    :param long num: Number to convert
    :return: A signed value
    """
    if num < 0:
        return num
    else:
        mask = (2 ** 64) - 1
        if num & (1 << (64 - 1)):
            return num | ~mask
        else:
            return num & mask

def chr_array_to_double(chr_array, start_pos, bytes_length):
    return unpack('>d', chr_array[start_pos: start_pos + bytes_length])[0]

def chr_array_to_float(chr_array, start_pos, bytes_length):
    return unpack('>f', chr_array[start_pos: start_pos + bytes_length])[0]

def chr_array_to_date(chr_array, start_pos):
    year = unpack('>h', chr_array[start_pos: start_pos + 2])[0]
    month = unpack('>h', chr_array[start_pos + 2: start_pos + 4])[0]
    day = unpack('>h', chr_array[start_pos + 4: start_pos + 6])[0]

    return datetime.date(year, month, day)

def chr_array_to_time(chr_array, start_pos):
    hour = unpack('>h', chr_array[start_pos: start_pos + 2])[0]
    minute = unpack('>h', chr_array[start_pos + 2: start_pos + 4])[0]
    sec = unpack('>h', chr_array[start_pos + 4: start_pos + 6])[0]
    msec = 0

    return datetime.time(hour, minute, sec, msec)

def chr_array_to_datetime(chr_array, start_pos):
    year = unpack('>h', chr_array[start_pos: start_pos + 2])[0]
    month = unpack('>h', chr_array[start_pos + 2: start_pos + 4])[0]
    day = unpack('>h', chr_array[start_pos + 4: start_pos + 6])[0]
    hour = unpack('>h', chr_array[start_pos + 6: start_pos + 8])[0]
    minute = unpack('>h', chr_array[start_pos + 8: start_pos + 10])[0]
    sec = unpack('>h', chr_array[start_pos + 10: start_pos + 12])[0]
    msec = unpack('>h', chr_array[start_pos + 12: start_pos + 14])[0]

    return datetime.datetime(year, month, day, hour, minute, sec, msec)

def chr_array_to_timestamp(chr_array, start_pos):
    year = unpack('>h', chr_array[start_pos: start_pos + 2])[0]
    month = unpack('>h', chr_array[start_pos + 2: start_pos + 4])[0]
    day = unpack('>h', chr_array[start_pos + 4: start_pos + 6])[0]
    hour = unpack('>h', chr_array[start_pos + 6: start_pos + 8])[0]
    minute = unpack('>h', chr_array[start_pos + 8: start_pos + 10])[0]
    sec = unpack('>h', chr_array[start_pos + 10: start_pos + 12])[0]
    msec = 0

    return datetime.datetime(year, month, day, hour, minute, sec, msec)

