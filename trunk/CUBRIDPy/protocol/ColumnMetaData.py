from CUBRIDPy.constants.CUBRID import CUBRID_DATA_TYPE

class ColumnMetaData(object):
    """
    ColumnMetaData class implementation
    """

    def _init_(self):
        """
        Class constructor
        :return:
        """
        self.column_type = None
        self.collection_element_type = CUBRID_DATA_TYPE.CCI_U_TYPE_UNKNOWN
        self.scale = -1
        self.precision = -1
        self.real_name = None
        self.table_name = None
        self.name = None
        self.is_nullable = False

        self.default_value = None
        self.is_auto_increment = False
        self.is_unique_key = False
        self.is_primary_key = False
        self.is_foreign_key = False
        self.is_reverse_index = False
        self.is_reverse_unique = False
        self.is_shared = False

