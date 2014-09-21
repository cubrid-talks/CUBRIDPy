class ResultInfo(object):
    """
    ResultInfo class implementation
    """

    def _init_(self):
        """
        Class constructor
        :return:
        """
        self.stmt_type = None
        self.result_count = 0
        self.oid = None
        self.cache_time_sec = 0
        self.cache_time_usec = 0
