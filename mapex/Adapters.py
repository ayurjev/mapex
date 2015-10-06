""" Модуль с адаптерами для подключения к СУБД """

from .Exceptions import AdapterException, DublicateRecordException
from .Sql import Adapter, PgDbField, MySqlDbField, MsSqlDbField, AdapterLogger
from .Mappers import FieldTypes
from .QueryBuilders import PgSqlBuilder, MySqlBuilder, MsSqlBuilder


class NoTableFound(Exception):
    pass


class PgSqlDbAdapter(Adapter):
    """ Адаптер для работы с PostgreSQL """
    def __init__(self):
        import postgresql.exceptions

        super().__init__()
        self.dublicate_record_exception = postgresql.exceptions.UniqueError

    # noinspection PyMethodMayBeStatic
    def get_query_builder(self):
        """ Возвращает объект для построения синтаксических конструкций специфичных для PostgreSQL """
        return PgSqlBuilder()

    # noinspection PyMethodMayBeStatic
    def open_connection(self, connection_data, autocommit=True):
        """
        Открывает соединение с базой данных
        :param connection_data: Данные для подключение к СУБД
        :return:               Экземпляр соединения
        """
        import postgresql
        import postgresql.exceptions

        try:
            return postgresql.open("pq://%s:%s@%s:%s/%s" % (
                connection_data[2], connection_data[3], connection_data[0], connection_data[1], connection_data[4])
            )
        except postgresql.exceptions.ClientCannotConnectError as e:
            if not "CODE: 53300" in str(e):
                raise

    def close_connection(self):
        """ Закрывает соединение с базой данных """
        self.connection.close()

    def execute_raw(self, sql):
        """
        Выполняет sql-сценарий. Неиспользует prepared statements
        @param sql: Текст sql-сценария
        @return: Результат выполнения
        """
        return self.connection.execute(sql)

    def execute_query(self, sql, params=None):
        """
        Выполняет sql-запрос и возвращает !генератор! для обхода результата выполнения запроса
        Запрос может быть параметризованным, если содержит плэйсхолдеры и params представляет собой список значений
        :param sql:         SQL-Запрос
        :param params:      Параметры для плейсхолдеров запроса
        """
        statement = self.connection.prepare(sql)
        *args, = params if params is not None else []
        try:
            for res in statement(*args):
                yield res
        except self.dublicate_record_exception as err:
            self.reconnect()
            raise DublicateRecordException(err)
        statement.close()

    def get_table_fields(self, table_name):
        """
        Возвращает информацию  о полях таблицы базы данных
        :param table_name:   Имя таблицы
        :return: :raise:    AdapterException
        """
        fields = {}
        primary = None
        schema = list(self.get_rows('''
            SELECT "column_name", "column_default", "is_nullable", "data_type", "character_maximum_length"
            FROM "information_schema"."columns" WHERE "table_name" = $1
        ''', [table_name]))
        if len(schema) == 0:
            raise NoTableFound("there is no table with name '%s' in current database" % table_name)
        for field in schema:
            fields[field[0]] = PgDbField(*field)

        constraints = list(self.get_rows('''
            SELECT "a"."column_name", "constraint_type", "column_default"
            FROM "information_schema"."constraint_column_usage" as "a"
            JOIN "information_schema"."table_constraints" as "b" ON ("a"."constraint_name" = "b"."constraint_name")
            JOIN "information_schema"."columns" as "c" ON ("c"."table_name", "c"."column_name") = ("a"."table_name", "a"."column_name")
            WHERE "a"."table_name" = $1
        ''', [table_name]))

        for constraint in constraints:
            if constraint[1] == "PRIMARY KEY":
                fields[constraint[0]].is_primary = True
                primary = constraint[0]

            if constraint[2] is not None and constraint[2].startswith("nextval"):
                fields[constraint[0]].autoincremented = True
            else:
                fields[constraint[0]].autoincremented = False

        return fields, primary

    @staticmethod
    def get_field_types_map():
        """ Возвращает словарь соответствий типов полей СУБД и типов полей, используемых маппером """
        return {
            FieldTypes.String: ["character", "text", "character varying", "str"],
            FieldTypes.Int: ["integer", "smallint", "bigint", "int"],
            FieldTypes.Float: ["double precision", "float"],
            FieldTypes.Bool: ["boolean", "bool"],
            FieldTypes.Date: ["date"],
            FieldTypes.DateTime: ["timestamp without time zone", "datetime"],
            FieldTypes.Time: ["time without time zone", "time"],
            FieldTypes.Bytes: ["bytea"]
        }

    def start_transaction(self):
        self.tx = self.connection.xact()
        self.tx.start()

    def commit(self):
        self.tx.commit()

    def rollback(self):
        self.tx.rollback()


class MySqlDbAdapter(Adapter):
    """ Адаптер для работы с MySQL """
    # noinspection PyDocstring
    from mysql.connector.errors import DatabaseError

    class TooManyConnectionsError(DatabaseError):
        """ Превышение ограничения количества соединений с MySQL """
        pass

    def __init__(self):
        import mysql.connector.errors

        super().__init__()
        mysql.connector.errors.custom_error_exception(1040, MySqlDbAdapter.TooManyConnectionsError)
        self.dublicate_record_exception = mysql.connector.errors.IntegrityError
        self.lost_connection_error = mysql.connector.errors.IntegrityError, mysql.connector.errors.OperationalError
        self.unread_result_error = mysql.connector.errors.InternalError
        self.no_table_connection = mysql.connector.errors.ProgrammingError

    # noinspection PyMethodMayBeStatic
    def get_query_builder(self):
        """ Возвращает объект для построения синтаксических конструкций специфичных для MySQL """
        return MySqlBuilder()

    # noinspection PyMethodMayBeStatic
    def open_connection(self, connection_data, autocommit=True):
        """
        Открывает соединение с базой данных
        :param connection_data: Данные для подключение к СУБД
        :return:               Экземпляр соединения
        """
        import mysql.connector

        try:
            return mysql.connector.connect(
                host=connection_data[0], port=connection_data[1],
                user=connection_data[2], password=connection_data[3],
                database=connection_data[4],
                autocommit=autocommit
            )
        except MySqlDbAdapter.TooManyConnectionsError:
            pass

    def close_connection(self):
        """ Закрывает соединение с базой данных """
        self.connection.close()

    def execute_raw(self, sql):
        """
        Выполняет sql-сценарий. Неиспользует prepared statements
        @param sql: Текст sql-сценария
        @return: Результат выполнения
        """
        try:
            cursor = self.connection.cursor()
        except self.lost_connection_error:
            self.reconnect()
            cursor = self.connection.cursor()
        except self.unread_result_error:
            self.reconnect()
            cursor = self.connection.cursor()

        result = list(cursor.execute(sql, multi=True))
        cursor.close()
        return result

    def execute_query(self, sql, params=None):
        """
        Выполняет sql-запрос и возвращает !генератор! для обхода результата выполнения запроса
        Запрос может быть параметризованным, если содержит плэйсхолдеры и params представляет собой список значений
        :param sql:         SQL-Запрос
        :param params:      Параметры для плейсхолдеров запроса
        """

        try:
            cursor = self.connection.cursor()
        except self.lost_connection_error:
            self.reconnect()
            cursor = self.connection.cursor()
        except self.unread_result_error:
            self.reconnect()
            cursor = self.connection.cursor()

        try:
            cursor.execute(sql, params if params is not None else [])
            if cursor.with_rows:
                for res in cursor:
                    yield res
            else:
                yield cursor.lastrowid
        except self.dublicate_record_exception as err:
            raise DublicateRecordException(err)
        finally:
            if self.connection.unread_result:
                self.connection.get_rows()
            cursor.close()

    def get_table_fields(self, table_name):
        """
        Возвращает информацию  о полях таблицы базы данных
        :param table_name:   Имя таблицы
        :return: :raise:    AdapterException
        """
        import mysql.connector.errors
        fields = {}
        primary = None
        try:
            schema = list(self.get_rows('''DESCRIBE %s''' % table_name))
        except self.no_table_connection as err:
            raise NoTableFound(err)
        for field in schema:
            fields[field[0]] = MySqlDbField(*field)
            if field[3] == "PRI":
                primary = field[0]
        return fields, primary

    @staticmethod
    def get_field_types_map():
        """ Возвращает словарь соответствий типов полей СУБД и типов полей, используемых маппером """
        return {
            FieldTypes.String: ["char", "varchar", "text", "tinytext", "mediumtext", "longtext"],
            FieldTypes.Int: ["tinyint", "smallint", "bigint", "mediumint", "int"],
            FieldTypes.Float: ["double", "float", "decimal"],
            FieldTypes.Bool: ["boolean", "bool"],
            FieldTypes.Date: ["date"],
            FieldTypes.DateTime: ["datetime"],
            FieldTypes.Time: ["time"],
            FieldTypes.Bytes: ["binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob"],
            FieldTypes.Enum: ["enum"]
        }

    def start_transaction(self):
        if not self.connection.in_transaction:
            try:
                self.connection.start_transaction()
            except self.lost_connection_error:
                self.reconnect()
                self.connection.start_transaction()

    def commit(self):
        if self.connection.in_transaction:
            self.connection.commit()

    def rollback(self):
        if self.connection.in_transaction:
            self.connection.rollback()


class MsSqlDbAdapter(Adapter):
    """ Адаптер для работы с MSSQL """
    def __init__(self):
        import pyodbc

        super().__init__()
        self.dublicate_record_exception = pyodbc.IntegrityError

    # noinspection PyMethodMayBeStatic
    def get_query_builder(self):
        """ Возвращает объект для построения синтаксических конструкций специфичных для MySQL """
        return MsSqlBuilder()

    # noinspection PyMethodMayBeStatic
    def open_connection(self, connection_data, autocommit=True):
        """
        Открывает соединение с базой данных
        :param connection_data: Данные для подключение к СУБД
        :return:               Экземпляр соединения
        """
        import pyodbc

        # noinspection PyUnresolvedReferences
        return pyodbc.connect('DSN=%s;UID=%s;PWD=%s;DATABASE=%s' % connection_data, autocommit=autocommit)

    def close_connection(self):
        """ Закрывает соединение с базой данных """
        self.connection.close()

    def execute_raw(self, sql):
        """
        Выполняет sql-сценарий. Неиспользует prepared statements
        @param sql: Текст sql-сценария
        @return: Результат выполнения
        """
        cursor = self.connection.cursor()
        cursor.execute(sql)

    def execute_query(self, sql, params=None):
        """
        Выполняет sql-запрос и возвращает !генератор! для обхода результата выполнения запроса
        Запрос может быть параметризованным, если содержит плэйсхолдеры и params представляет собой список значений
        :param sql:         SQL-Запрос
        :param params:      Параметры для плейсхолдеров запроса
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params if params is not None else [])
        except self.dublicate_record_exception as err:
            raise DublicateRecordException(err)
        if cursor.rowcount == 0:
            return
        elif cursor.rowcount == -1:
            for res in cursor:
                yield res
        else:
            yield cursor.execute('''SELECT @@IDENTITY''').fetchone()

    def get_table_fields(self, table_name):
        """
        Возвращает информацию  о полях таблицы базы данных
        :param table_name:   Имя таблицы
        :return: :raise:    AdapterException
        """
        fields = {}
        primary = None
        schema = list(self.get_rows('''
        SELECT
        INFORMATION_SCHEMA.COLUMNS.COLUMN_NAME,
        INFORMATION_SCHEMA.COLUMNS.IS_NULLABLE,
        INFORMATION_SCHEMA.COLUMNS.DATA_TYPE,
        INFORMATION_SCHEMA.COLUMNS.CHARACTER_MAXIMUM_LENGTH,
        INFORMATION_SCHEMA.COLUMNS.COLUMN_DEFAULT,
        INFORMATION_SCHEMA.KEY_COLUMN_USAGE.CONSTRAINT_NAME,
        COLUMNPROPERTY(object_id(INFORMATION_SCHEMA.COLUMNS.TABLE_NAME), INFORMATION_SCHEMA.COLUMNS.COLUMN_NAME, 'IsIdentity')
        FROM INFORMATION_SCHEMA.COLUMNS
        LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ON (
            INFORMATION_SCHEMA.COLUMNS.COLUMN_NAME = INFORMATION_SCHEMA.KEY_COLUMN_USAGE.COLUMN_NAME AND
            INFORMATION_SCHEMA.COLUMNS.TABLE_NAME = INFORMATION_SCHEMA.KEY_COLUMN_USAGE.TABLE_NAME
        )
        WHERE INFORMATION_SCHEMA.COLUMNS.TABLE_NAME = '%s' ''' % table_name))
        if schema == []:
            raise NoTableFound()
        for field in schema:
            fields[field[0]] = MsSqlDbField(*field)
            if field[5] and field[5].find("PK__") > -1:
                primary = field[0]
        return fields, primary

    @staticmethod
    def get_field_types_map():
        """ Возвращает словарь соответствий типов полей СУБД и типов полей, используемых маппером """
        return {
            FieldTypes.String: ["nchar", "nvarchar", "ntext", "char", "varchar", "text"],
            FieldTypes.Int: ["tinyint", "smallint", "bigint", "mediumint", "int"],
            FieldTypes.Float: ["real", "float", "money", "numeric", "smallmoney", "Decimal", "decimal"],
            FieldTypes.Bool: ["bit"],
            FieldTypes.Date: ["date"],
            FieldTypes.DateTime: ["datetime", "datetime2"],
            FieldTypes.Time: ["time"],
            FieldTypes.Bytes: ["varbinary"]

        }

    def start_transaction(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass


# noinspection PyUnusedLocal
class MongoDbAdapter(AdapterLogger):
    """ Адаптер для работы с MongoDB """

    def __init__(self):
        import pymongo.errors

        super().__init__()
        self.db = None

        self.dublicate_record_exception = pymongo.errors.DuplicateKeyError
        self.update_primary_exception = pymongo.errors.OperationFailure

    def connect(self, connection_data: tuple, autocommit=True):
        """ Выполняет подключение к СУБД по переданным реквизитам
        @param connection_data: host, port, database
        """
        import pymongo
        import pymongo.errors

        try:
            self.db = pymongo.MongoClient(connection_data[0], connection_data[1])[connection_data[2]]
            return self
        except pymongo.errors.ConnectionFailure as e:
            if "[Errno 104]" in str(e):
                return False
            raise

    def close(self):
        """ Закрывает соединение с базой данных """
        pass

    def count_query(self, collection_name: str, conditions: dict, joined_tables) -> int:
        """
        Выполняет запрос на подсчет строк в таблице по заданным условиям
        @param collection_name: Имя коллекции
        @type collection_name: str
        @param conditions: Условия выборки записей для подсчета строк
        @type conditions: dict
        @param joined_tables: Словарь с данными о присоединенными таблицами (В данном адаптере не используется)
        @type: dict
        @return : Количество записей в коллекции, соответствующих условиям
        @rtype : int

        """
        if self.query_analyzer:
            self.query_analyzer.log("count", conditions)
        return self.db[collection_name].find(conditions).count()

    def insert_query(self, collection_name: str, data: dict, primary_key):
        """
        Выполняет запрос на вставку записей в коллекцию
        @param collection_name: Имя коллекции
        @type collection_name: str
        @param data: Данные для вставки
        @param primary_key: Первичный ключ коллекции
        """
        if self.query_analyzer:
            self.query_analyzer.log("insert", data)
        try:
            return self.db[collection_name].insert(data)
        except self.dublicate_record_exception as err:
            raise DublicateRecordException(err)

    def select_query(self, collection_name: str, fields: list, conditions: dict, params=None):
        """
        Выполняет запрос на получение записей из базы
        @param collection_name: Имя коллекции
        @param fields: Запрашиваемые к возврату поля
        @param conditions: Условия выборки записей
        @param params: Параметры выборки записей
        @return:
        """
        if len(fields) > 0:
            fields = {field: True for field in fields}
            if "_id" not in fields:
                fields["_id"] = False
        else:
            fields = None

        if self.query_analyzer:
            self.query_analyzer.log("select", (collection_name, fields, conditions, params))
        return self.db[collection_name].find(
            conditions, fields,
            limit=params.get("limit", 0) if params else 0,
            sort=[self.fix_sorting(params.get("order"))] if params and params.get("order") else None
        )

    def delete_query(self, collection_name: str, conditions: dict, joined_tables=None):
        """
        Выполняет запрос на удаление записей из коллекции
        @param collection_name: Имя коллекции
        @param conditions: Условия удаление записей
        @param joined_tables: Список присоединенных таблиц с уловия присоединения (Не используется)
        @return:
        """
        if self.query_analyzer:
            self.query_analyzer.log("delete", (collection_name, conditions))
        res = self.db[collection_name].remove(conditions)
        return res

    def update_query(self, collection_name: str, data: dict, conditions: dict, params: dict, joined_tables, primary_key):
        """
        Выполняет запрос на обновление данных в коллекции
        @param collection_name: Имя коллекции
        @param data: Данные для обновления
        @param conditions: Условия обновления записей
        @param params: Параметры обновления записей
        @param joined_tables: Список присоединенных таблиц с уловия присоединения (Не используется)
        @param primary_key: Первичный ключ коллекции (Не используется)
        @return:
        """
        conditions = {} if conditions is None else conditions
        if self.query_analyzer:
            self.query_analyzer.log("update", (collection_name, data, conditions))
        try:
            return self.db[collection_name].update(conditions, {"$set": data}, multi=True)

        # Вот это, разумеется, некорректно...
        # но пока я не знаю что с этим делать, так как в SQL изменять primary можно...
        # В одном из тестов я меняю поле id на уже существующее, чтобы отловить DublicateRecordException
        except self.update_primary_exception as err:
            raise DublicateRecordException(err)

    def fix_sorting(self, val):
        """
        Конвертирует параметры сортировки к формату, используемому в mongodb
        @param val: Значение для конвертации (может быть tuple или list of tuples)
        @return:
        """
        import pymongo

        fixer = lambda v: pymongo.DESCENDING if v.upper() == "DESC" else pymongo.ASCENDING
        if type(val) is tuple:
            val = val[0], fixer(val[1])
        elif type(val) is list:
            val = [self.fix_sorting(v) for v in val]
        return val

    @staticmethod
    def get_table_fields(table_name):
        """
        Возвращает скромное описание полей коллекции. Чего еще ожидать от schemaless...
        @param table_name: Имя коллекции
        @return:
        """
        class NoSqlField(object):
            def __init__(self):
                self.autoincremented = True

        return {"_id": NoSqlField()}, "_id"

    def start_transaction(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass
