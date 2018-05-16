import pymysql
import pydb.database as database


class SQLDatabase(database.Database):

    def __init__(self, name, table=None, **kwargs):
        """
        Inherits Database class. Sets type to "SQL"

        :param name: name of database. This is not a label but the actual database name
        :param connectionData: connectionData object with required information for server/database connection
        :param table:
        :param dryRun:
        """
        super(SQLDatabase, self).__init__(name=name, dbtype='SQL', table=table, **kwargs)

    def _connect(self):
        """

        :return:
        """
        return pymysql.connect(host=self.connectionData.address,
                               port=self.connectionData.port,
                               user=self.connectionData.user,
                               passwd=self.connectionData.passwd,
                               db=self.name)

    def _raw_database_call(self, statement, use_cache=True):
        return self.execute_sql(statement=statement, use_cache=use_cache)

    def execute_sql(self, statement, use_cache=True):
        """

        :param statement:
        :param use_cache: if True will attempt to use stored cache instead of accessing the pydb
        :return:
        """
        # print('\nsql: {}\n'.format(statement))

        try:
            return self._use_cache(statement)
        except KeyError:
            pass

        if self.dryRun:
            return

        result = None
        conn = None
        cursor = None
        try:
            conn = self._connect()
            cursor = conn.cursor()
            status = cursor.execute(statement)
            conn.commit()

            if status:
                result = cursor.fetchall()

        except Exception as e:
            print(str(e))

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        clean_result = self.clean_result(result)

        self._store_cache(statement, clean_result)

        return clean_result

    @staticmethod
    def clean_result(value):
        """

        :param value:
        :return:
        """
        if value and len(value) == 1:
            if len(value[0]) == 1:
                return value[0][0]
            else:
                return value[0]

        return value

    def query(self, field, **kwargs):
        """

        :param field:
        :param kwargs:
        :return:
        """
        if field is None:
            field = '*'

        args = []
        for key, val in kwargs.items():
            args.append("{} = '{}'".format(key, val))
        if kwargs:
            return self.execute_sql("SELECT {} FROM {} WHERE {}".format(field, self.table, " AND ".join(args)))
        else:
            return self.execute_sql("SELECT {} FROM {}".format(field, self.table))

    def query_like(self, field, **kwargs):
        """

            :param field:
            :param kwargs:
            :return:
        """
        args = []
        for key, val in kwargs.items():
            args.append("{} LIKE '%{}%'".format(key, val))

        return self.execute_sql("SELECT {} FROM {} WHERE {}".format(field, self.table, " AND ".join(args)))

    def update(self, field, value, **kwargs):
        """

        :param field:
        :param value:
        :param kwargs:
        :return:
        """
        if not kwargs:
            raise RuntimeError("Must set kwargs. Update without condition too dangerous.")

        args = []
        for key, val in kwargs.items():
            args.append("{} = '{}'".format(key, val))

        condition = " AND ".join(args)
        return self.execute_sql("UPDATE {} SET {} = '{}' WHERE {}".format(self.table, field, value, condition))

    def insert(self, **kwargs):
        """

        :param kwargs:
        :return:
        """
        columns = []
        values = []

        for key, val in kwargs.items():
            columns.append(key)
            values.append(val)

        column_value_data = "({}) VALUES ('{}')".format(", ".join(columns), "', '".join(values))

        return self.execute_sql("INSERT INTO {} {}".format(self.table, column_value_data))

    def delete(self, **kwargs):
        """

        :param kwargs:
        :return:
        """
        args = []
        for key, val in kwargs.items():
            args.append("{} = '{}'".format(key, val))

        return self.execute_sql("DELETE FROM {} WHERE {}".format(self.table, " AND ".join(args)))

    def _store_cache(self, statement, value):
        """
        Store the returned value alongside the SQL cached used.
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


if __name__ == '__main__':
    import pydb.connection as connection
    cd = connection.ConnectionData(user='testUser', passwd='password')

    db = database.Database(name='production', connectionData=cd, dryRun=False)
    db.table = 'asset'
    db.insert(name='ball')

    print(db.query('name'))


    #print(pydb.insert(name='myTestJob', user='test.user'))
    #pydb.update(field='user', value='newUser', ID=12)
    #print(db.query('name', user='newUser'))
