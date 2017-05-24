import platform

__author__ = 'wangcx'


class Properties(object):
    def __init__(self):
        pass

    def getProperties(self, fileName):
        properties = {}
        try:
            pro_file = open(fileName, 'r')
            try:
                os = platform.system().lower()
                sep = '\n'
                # is equal or ==
                if os == "windows":
                    sep = '\r\n'
                elif os == "linux":
                    sep = '\n'

                for line in pro_file:
                    if line.find('=') > 0:
                        strs = line.replace(sep, '').split('=')
                        properties[strs[0]] = strs[1]
            finally:
                pro_file.close()
        except Exception, e:
            print e

        return properties
