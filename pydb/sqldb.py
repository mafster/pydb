import warnings

import pymysql

import pydb.database as database


class SQLDatabase(database.Database):

    def __init__(self, name, db_schema, **kwargs):
        """
        Inherits Database class. Sets type to "SQL"

        :param name: name of database. This is not a label but the actual database name
        :param connectionData: connectionData object with required information for server/database connection
        :param table:
        :param dryRun:
        """
        super(SQLDatabase, self).__init__(name=name, database_type='SQL', db_schema=db_schema, **kwargs)

    @property
    def table(self):
        """ For SQL style language """
        return self.db_schema

    def _connect(self):
        """

        :return:
        """
        return pymysql.connect(host=self.connectionData.address,
                               port=self.connectionData.port,
                               user=self.connectionData.username,
                               passwd=self.connectionData.password,
                               db=self.name)

    def raw_database_call(self, statement, use_cache=True, clean_result=False):
        return self.execute_sql(statement=statement, clean_result=clean_result)

    def execute_sql(self, statement, data_as_dict=False, clean_result=False):
        """

        :param statement:   *(str)* SQL statement
        :param use_cache:   *(bool)* if True will attempt to use stored cache instead of accessing the pydb
        :param data_as_dict:*(bool)* if True will return result as dictionary for one result or list of dictionaries
        :return:            *(bool)* False if failed
        """
        # print('\nsql: {}\n'.format(statement))

        #try:
        #    return self._use_cache(statement)
        #except KeyError:
        #    pass

        if self.dryRun:
            return

        result = None
        conn = None
        cursor = None
        try:
            conn = self._connect()

            if data_as_dict is True:
                cursor = conn.cursor(pymysql.cursors.DictCursor)
            else:
                cursor = conn.cursor()

            status = cursor.execute(statement)
            conn.commit()

            if status:
                result = cursor.fetchall()

        except Exception as e:
            warnings.warn(e)

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        if clean_result:
            result = self.clean_result(result)

        self._store_cache(statement, result)

        return result

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

    def find(self, fields=None, data_as_dict=True, clean_result=False, **kwargs):
        """

        :param fields:          *(str, list(str))* the fields to find if None will find all '*'
        :param data_as_dict:    *(bool)* if True will return result as dictionary for one result or list of dictionaries
        :param distinct:        *(bool)* if True will return unqiue results only
        :param order_by:        *(str)* will order the results by the field/key passed
        :param clean_result:    *(bool)* if True will clean the result. Useful for calls will result in only 1 item
        :param kwargs:
        :return:
        """
        distinct = kwargs.pop('distinct', False)
        order_by = kwargs.pop('order_by', None)

        if isinstance(order_by, (list, tuple)):
            order_by = '{}'.format(', '.join(order_by))

        if fields is None:
            fields = '*'

        if isinstance(fields, (list, tuple)):
            fields = '{}'.format(', '.join(fields))

        args = []
        for key, val in kwargs.items():
            args.append("{} = '{}'".format(key, val))

        if kwargs:
            statement = "SELECT {} FROM {} WHERE {}".format(fields, self.table, " AND ".join(args))
        else:
            statement = "SELECT {} FROM {}".format(fields, self.table)

        if distinct is True:
            statement = statement.replace('SELECT', 'SELECT DISTINCT')

        if order_by:
            statement = '{} ORDER BY {} ASC'.format(statement, order_by)  # defaults order to "ascending"

        return self.execute_sql(statement=statement, data_as_dict=data_as_dict, clean_result=clean_result)

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

    def update(self, data=None, field=None, value=None, **kwargs):
        """
        Update entries that match kwargs passed. Can take input as dict (param data) or as field-value params

        :param data:    *(dict)*         data takes preference over field, value
        :param field:   *(list, str)*    list of fields to update
        :param value:   *(list, object)* list of values to add to fields passed (order must match)
        :param kwargs:  *(**dict)*       conditions on which to update. Method will raise error if no kwargs passed
        :return:
        """
        if not kwargs:
            raise RuntimeError("Must set kwargs. Update without condition too dangerous.")

        set_data = []

        if data:
            # Convert data param to list of "key=value" pairs
            for key, val in data.items():
                if key and val:
                    set_data.append("{} = '{}'".format(key, val))

        else:
            if not field or not value:
                raise RuntimeError('must pass both params "field" and "value" or use "data" param')
            set_data.append('{} = {}'.format(field, value))

        set_data = ', '.join(set_data)  # convert to comma-separated

        args = []
        for key, val in kwargs.items():
            args.append("{} = '{}'".format(key, val))

        condition = " AND ".join(args)
        return self.execute_sql("UPDATE {} SET {} WHERE {}".format(self.table, set_data, condition))

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
