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
        super(MongoDatabase, self).__init__(name=name, database_type='mongo', **kwargs)

        self._connect()

    @property
    def db(self):
        """ Returns the mongodb database object for further CRUD operations """
        return getattr(self.client, self.database)

    def _connect(self):
        self.client = MongoClient(host=self.connectionData.address,
                                  port=self.connectionData.port,
                                  username=self.connectionData.username,
                                  password=self.connectionData.password,
                                  authSource=self.connectionData.authSource,
                                  authMechanism=self.connectionData.authMechanism,)

    def query(self, **kwargs):
        """

        :param kwargs:
        :return:
        """
        collection = getattr(self.db, self.db_schema)
        cur = collection.find(kwargs)

        return list(cur)

    def update(self, data, **kwargs):
        """
        Update a database document/entry by key referencing **kwargs

        :param field:
        :param value:
        :param kwargs:
        :return:
        """

        new_values = {"$set": kwargs}

        collection = getattr(self.db, self.db_schema)
        print('updating {} with {}'.format(data, new_values))
        result = collection.update_one(data, new_values)
        print(result)

    def insert(self, **kwargs):
        """
        Insert a new database entry

        :param kwargs:
        :return:
        """
        collection = getattr(self.db, self.db_schema)
        post_id = collection.insert_one(kwargs).inserted_id

        return post_id
