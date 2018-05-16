from pydb import connection

try:
    from pydb import sqldb
except ImportError as e:
    pass

try:
    from pydb import mongodb
except ImportError as e:
    pass

# TODO: This needs to be read from secure config py file?
cd = connection.ConnectionData(user='root', passwd='unipipepassword')


def get_database_object(dbtype, name, **kwargs):

    if dbtype.upper() == 'SQL':
        return sqldb.SQLDatabase(name=name, connectionData=cd, **kwargs)
    elif dbtype.upper() == 'MONGO':
        return mongodb.MongoDatabase(name=name, connectionData=cd, **kwargs)
    else:
        raise RuntimeError('Incorrect dbtype passed')
