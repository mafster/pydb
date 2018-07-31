import pydb.database as database

from pymongo import MongoClient


class MongoDatabase(database.Database):

    def __init__(self, name, **kwargs):
        """
        Inherits Database class. Sets type to "MONGO"

        :param name: name of database. This is not a label but the actual database name
        :param connectionData: connectionData object with required information for server/database connection
        :param dryRun:
        """
        super(MongoDatabase, self).__init__(name=name, dbtype='MONGO', **kwargs)

        self.client = MongoClient(port=self.connectionData.port)

    @property
    def db(self):
        """ Returns the mongodb database object for further CRUD operations """
        return getattr(self.client, self.database)

    def _raw_database_call(self, statement, use_cache=True):
        pass

    def _connect(self):
        self.client = MongoClient(port=self.connectionData.port)

    @classmethod
    def clean_result(cls):
        """

        :param value:
        :return:
        """
        raise NotImplementedError('Implement in subclass')

    def find_one(self, table, **kwargs):
        if len(kwargs.keys()) > 1:
            raise RuntimeError('too many keywords passed')

        queryDict = {kwargs.keys()[0]: kwargs.values()[0]}

        return getattr(self.db, table).find_one(queryDict)

    def query(self, field, **kwargs):
        """

        :param field:
        :param kwargs:
        :return:
        """
        self.db

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
