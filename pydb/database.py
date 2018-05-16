import pydb.connection as connection


class Database(object):
    """
    Generic abstract database object. Define database calls in subclasses.
    Typical usage would be of subclass only.

    """
    def __init__(self, name, dbtype=None, table=None, connectionData=None, dryRun=False):
        """

        :param name: name of database. This is not a label but the actual database name
        :param dbtype: type of database being used e.g. "SQL", "MONGODB"
        :param connectionData: connectionData object with required information for server/database connection
        :param table:
        :param dryRun:
        """
        self.name = name
        self.dbtype = dbtype
        self.connectionData = connectionData
        self.table = table
        self.dryRun = dryRun

        self._cache = {}

    def __str__(self):
        if self.dbtype:
            return '{}/{}/{}'.format(self.__class__.__name__, self.name, self.dbtype)
        else:
            return '{}/{}'.format(self.__class__.__name__, self.name)

    def __repr__(self):
        return '<{}(object)> - db:{}, type:{}, table:{})'.format(self.__class__.__name__, self.name, self.dbtype, self.table)

    @property
    def database(self):
        return self.name

    @property
    def database_type(self):
        return self.dbtype

    def _connect(self):
        """
        Connect to the database. For example pymysql.connect()
        :return: return the connection object

        """
        raise NotImplementedError('Implement in subclass')

    def _raw_database_call(self, statement, use_cache=True):
        """
        database call in native language. For example pymysql.connect() && pymsql.execute(statement)

        :param statement: raw string statement to process
        :param use_cache: if True will attempt to use stored cache instead of accessing the database
        :return: any data or logs returned from database

        cache will be handled per subclass along with result processing, connection and finishing up request

        Example may follow:
        0. Check cache for results if not result then...
        1. connect
        2. send query
        3. process results
        4. close connection
        5. update cache

        """
        raise NotImplementedError('Implement in subclass')

    @classmethod
    def clean_result(cls):
        """

        :param value:
        :return:
        """
        raise NotImplementedError('Implement in subclass')

    def query(self, field, **kwargs):
        """

        :param field:
        :param kwargs:
        :return:
        """
        raise NotImplementedError('Implement in subclass')

    def query_like(self, field, **kwargs):
        """

            :param field:
            :param kwargs:
            :return:
        """
        raise NotImplementedError('Implement in subclass')

    def update(self, field, value, **kwargs):
        """
        Update a database entry by key referencing **kwargs

        :param field:
        :param value:
        :param kwargs:
        :return:
        """
        raise NotImplementedError('Implement in subclass')

    def insert(self, **kwargs):
        """
        Insert a new database entry

        :param kwargs:
        :return:
        """
        raise NotImplementedError('Implement in subclass')

    def delete(self, **kwargs):
        """

        :param kwargs:
        :return:
        """
        raise NotImplementedError('Implement in subclass')

    def _store_cache(self, statement, value):
        """
        Store the returned value alongside the cached used.

        :param statement:
        :param value:
        :return:
        """
        self._cache[statement] = value

    def _use_cache(self, statement):
        """
        Attempts to use cache if stored
        :param statement: SQL statement to look up
        :return:
        """
        return self._cache[statement]


def get_database(**kwargs):
    """
    wraps the Database object and returns with subclass of Database.
    This is because setting the dbtype directly wont return the subclass

    :param kwargs: same kwargs as defined in Database class
    :return: subclass of DatabaseObject
    """
    dbtype = kwargs.pop('dbtype')
    if dbtype == 'SQL':
        return Database(**kwargs)



if __name__ == '__main__':
    cd = connection.ConnectionData(user='testUser', passwd='password')

    # Instantiate a Database object not very useful as there's no dbtype defined
    db = Database(name='production', connectionData=cd, dryRun=False)
    print(db)
    print(db.__repr__())
