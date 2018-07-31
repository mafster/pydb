class ConnectionData(object):

    def __init__(self, username, password, address, port):
        """

        :param username:
        :param password:
        :param address:
        :param port:
        """
        self.username = username
        self.password = password
        self.address = address
        self.port = int(port)  # convert port to integer
