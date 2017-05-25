import datetime

__author__ = 'kevin'


class DateUtil:
    @staticmethod
    def get_ymdhms_now():
        now = datetime.datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        return now_str

    @staticmethod
    def get_ymd_now():
        now = datetime.datetime.now()
        # now_str = now.strftime("%Y%m%d%H%M%S")
        now_str = now.strftime("%Y%m%d")
        return now_str

    @staticmethod
    def date_to_str(date):
        date_str = None
        try:
            # s = datetime.datetime.strptime(date,"%Y-%m-%d %H:%M:%S.%f")
            date_str = datetime.datetime.strftime(date, "%Y-%m-%d")
        except Exception, e:
            print(e.__str__())
        return date_str

    @staticmethod
    def str_to_date(date_str):
        date = None
        try:
            # s = datetime.datetime.strptime(date,"%Y-%m-%d %H:%M:%S.%f")
            date = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        except Exception, e:
            print(e.__str__())
        return date

    @staticmethod
    def date_diff(x, k):
        dateStart = None
        dateEnd = None

        if x != None and x != '' and x != 'NULL':
            dateStart = DateUtil.str_to_date(x)
        if k != None and k != '' and k != 'NULL':
            dateEnd = DateUtil.str_to_date(k)

        if dateStart != None and dateEnd != None:
            return (dateStart - dateEnd).days
        else:
            return 0

    @staticmethod
    def timestamp_diff(x, k):
        dateStart = None
        dateEnd = None

        if x != None and x != '' and x != 'null':
            dateStart = DateUtil.str_to_date(x)
        if k != None and k != '' and k != 'null':
            dateEnd = DateUtil.str_to_date(k)

        if dateStart != None and dateEnd != None:
            return (dateEnd - dateStart).seconds
        else:
            return 0
