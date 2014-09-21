"""
This module contains functions for dealing with messages localization
"""

import enUS.ERRORS as en_US_ERRORS
import roRO.ERRORS as ro_RO_ERRORS
from ERROR_ID import *

def _search_error_code(error_code, list_a, list_b, list_c):
    """
    Search an error code and returns the corresponding error message.
    :param int error_code: Error code
    :param dict list_a: List to search in
    :param dict list_b: Another list to search in
    :param dict list_c: Yet another list to search in
    :return string: An error message or empty string.
    """
    if error_code in list_a:
        return list_a[error_code]
    else:
        if error_code in list_b:
            return list_b[error_code]
        else:
            if error_code in list_c:
                return list_c[error_code]

    return ''


def get_error_message(locale, error_code = ERROR_ID.NO_ERROR):
    """
    Retrieves a localized error message.
    :param string locale: Locale. f not found, it will default to enUS
    """

    # If localization is wrong, default to en-US
    if locale.lower() not in ('en-us', 'ro-ro'):
        locale = 'en-us'

    if error_code == 0:
        return ''

    if locale.lower() == 'en-us':
        return _search_error_code(error_code,
            en_US_ERRORS.ERRORS.DRIVER_ERRORS,
            en_US_ERRORS.ERRORS.CAS_ERRORS,
            en_US_ERRORS.ERRORS.CUBRID_ERRORS)
    elif locale.lower() == 'ro-ro':
        return _search_error_code(error_code,
            ro_RO_ERRORS.ERRORS.DRIVER_ERRORS,
            ro_RO_ERRORS.ERRORS.CAS_ERRORS,
            ro_RO_ERRORS.ERRORS.CUBRID_ERRORS)

    return ''
