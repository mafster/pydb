from typing import Any

from pymongo.results import UpdateResult

import pydb.database as database

from pymongo import MongoClient


class MongoDatabase(database.Database):

    def __init__(self, name: str, db_schema: str, connection_string: str):
        """
        Inherits Database class. Sets type to "MONGO"

        :param name: name of database. This is not a label but the actual database name
        :param connection_string: mongo connectionString
        """
        super(MongoDatabase, self).__init__(name=name, database_type='mongo', db_schema=db_schema)

        self.connection_string = connection_string
        self._connect()

    @property
    def db(self):
        """ Returns the mongodb database object for further CRUD operations """
        return getattr(self.client, self.database)

    def _connect(self):
        self.client = MongoClient(self.connection_string)

    def find(self, **kwargs) -> list[Any]:
        """

        :param kwargs:
        :return:
        """
        _filter = kwargs.pop('filter', {})
        projection = kwargs.pop('projection', {})
        print('db - filter', _filter)
        print('db - projection', projection)
        collection = getattr(self.db, self.db_schema)
        cur = collection.find(filter=_filter, projection=projection, **kwargs)

        return list(cur)

    def update(self, _filter, update, **kwargs) -> UpdateResult:
        """
        Update a database document/entry by key referencing **kwargs

        :param _filter:
        :param update:
        :param kwargs:
        :return:
        """

        collection = getattr(self.db, self.db_schema)
        print(f'db - filter {_filter}\ndb - update {update}')
        result = collection.update_one(_filter, update, **kwargs)
        return result

    def insert(self, **kwargs) -> str:
        """
        Insert a new database entry

        :param kwargs:
        :return:
        """
        collection = getattr(self.db, self.db_schema)
        post_id = collection.insert_one(kwargs).inserted_id

        return post_id
