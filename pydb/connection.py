class ConnectionData(object):

    def __init__(self, user, passwd, address='localhost', port=3306):
        """

        :param user:
        :param passwd:
        :param address:
        :param port:
        """
        self.user = user
        self.passwd = passwd
        self.address = address
        self.port = port
