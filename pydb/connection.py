class ConnectionData:

    def __init__(self, username, password, address, port, **kwargs):
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

        for key, val in kwargs.items():
            setattr(self, key, val)
