from pydb import connection

try:
    from . import sqldb
except ImportError as e:
    pass

try:
    from . import mongodb
except ImportError as e:
    pass

# TODO: This needs to be read from secure config py file?
cd = connection.ConnectionData(user='unipipeuser', passwd='unipipepassword')


def get_database_object(database_type, name, **kwargs):

    if database_type.upper() == 'SQL':
        return sqldb.SQLDatabase(name=name, connectionData=cd, **kwargs)
    elif database_type.upper() == 'MONGO':
        return mongodb.MongoDatabase(name=name, connectionData=cd, **kwargs)
    else:
        raise RuntimeError('Unknown database_type passed')
