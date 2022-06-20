import warnings
from typing import TypeVar, Union, Generic

from pydb import connection
from .mongodb import MongoDatabase
from .sqldb import SQLDatabase

try:
    from . import sqldb
except ImportError as e:
    warnings.warn(str(e))
    sqldb = None

try:
    from . import mongodb
except ImportError as e:
    warnings.warn(str(e))
    mongodb = None

if sqldb is None and mongodb is None:
    raise RuntimeError('No available database python API. Please install one')

T = TypeVar('T', MongoDatabase, SQLDatabase)


def get_database_object(database_type: str, name: str, db_schema: str, **kwargs) -> Union[MongoDatabase, SQLDatabase]:
    """
    :param database_type:   *(str)* type of database e.g. "sql"
    :param name:            *(str)* name of the database e.g. 'production'
    :param db_schema:       *(str)* the sub category. in sql this would be the "table" name
    :param kwargs:          *(**dict)* connection data
    :return:
    """

    if database_type == 'sql':
        cd = connection.ConnectionData(**kwargs)
        return SQLDatabase(name=name, db_schema=db_schema, connectionData=cd)
    elif database_type == 'mongo':
        return MongoDatabase(name=name, db_schema=db_schema, connection_string=kwargs.get('connection_string'))
    else:
        raise RuntimeError('Unknown database_type passed')
