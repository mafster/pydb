from pydb import connection

try:
    from . import sqldb
except ImportError as e:
    pass

try:
    from . import mongodb
except ImportError as e:
    pass


def get_database_object(database_type, name, db_schema, **kwargs):
    """

    :param database_type:   *(str)* type of database e.g. "sql"
    :param name:            *(str)* name of the database e.g. 'production'
    :param db_schema:       *(str)* the sub category. in sql this would be the "table" name
    :param kwargs:          *(**dict)* connection data
    :return:
    """
    cd = connection.ConnectionData(**kwargs)

    if database_type == 'sql':
        return sqldb.SQLDatabase(name=name, db_schema=db_schema, connectionData=cd)

    elif database_type == 'mongo':
        return mongodb.MongoDatabase(name=name, db_schema=db_schema, connectionData=cd)
    else:
        raise RuntimeError('Unknown database_type passed')
