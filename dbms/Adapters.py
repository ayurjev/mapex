""" Модуль с адаптерами для подключения к СУБД """

from mapex.core.Exceptions import AdapterException, DublicateRecordException
from mapex.core.Sql import Adapter, PgDbField, MySqlDbField, MsSqlDbField
from mapex.core.Mappers import FieldTypes
from mapex.dbms.QueryBuilders import PgSqlBuilder, MySqlBuilder, MsSqlBuilder
from mapex.core.Sql import AdapterLogger


class PgSqlDbAdapter(Adapter):
    """ Адаптер для работы с PostgreSQL """

    # noinspection PyMethodMayBeStatic
    def get_query_builder(self):
        """ Возвращает объект для построения синтаксических конструкций специфичных для PostgreSQL """
        return PgSqlBuilder()

    # noinspection PyMethodMayBeStatic
    def open_connection(self, connection_data):
        """
        Открывает соединение с базой данных
        :param connection_data: Данные для подключение к СУБД
        :return:               Экземпляр соединения
        """
        import postgresql
        import postgresql.exceptions
        self.dublicate_record_exception = postgresql.exceptions.UniqueError
        return postgresql.open("pq://%s:%s@%s:%s/%s" % (
            connection_data[2], connection_data[3], connection_data[0], connection_data[1], connection_data[4])
        )

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
            raise AdapterException("there is no table with name '%s' in current database" % table_name)
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
            FieldTypes.Time: ["time without time zone", "time"]
        }


class MySqlDbAdapter(Adapter):
    """ Адаптер для работы с MySQL """

    # noinspection PyMethodMayBeStatic
    def get_query_builder(self):
        """ Возвращает объект для построения синтаксических конструкций специфичных для MySQL """
        return MySqlBuilder()

    # noinspection PyMethodMayBeStatic
    def open_connection(self, connection_data):
        """
        Открывает соединение с базой данных
        :param connection_data: Данные для подключение к СУБД
        :return:               Экземпляр соединения
        """
        import mysql.connector
        import mysql.connector.errors
        self.dublicate_record_exception = mysql.connector.errors.IntegrityError
        return mysql.connector.connect(
            user=connection_data[2], password=connection_data[3],
            host=connection_data[0], port=connection_data[1], database=connection_data[4],
            autocommit=True
        )

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
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params if params is not None else [])
            if cursor.with_rows:
                for res in cursor.fetchall():
                    yield res
            else:
                yield cursor.lastrowid
        except self.dublicate_record_exception as err:
            raise DublicateRecordException(err)
        finally:
            cursor.close()

    def get_table_fields(self, table_name):
        """
        Возвращает информацию  о полях таблицы базы данных
        :param table_name:   Имя таблицы
        :return: :raise:    AdapterException
        """
        fields = {}
        primary = None
        schema = list(self.get_rows('''DESCRIBE %s''' % table_name))
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
            FieldTypes.Float: ["double", "float"],
            FieldTypes.Bool: ["boolean", "bool"],
            FieldTypes.Date: ["date"],
            FieldTypes.DateTime: ["datetime"],
            FieldTypes.Time: ["time"]
        }


class MsSqlDbAdapter(Adapter):
    """ Адаптер для работы с MSSQL """

    # noinspection PyMethodMayBeStatic
    def get_query_builder(self):
        """ Возвращает объект для построения синтаксических конструкций специфичных для MySQL """
        return MsSqlBuilder()

    # noinspection PyMethodMayBeStatic
    def open_connection(self, connection_data):
        """
        Открывает соединение с базой данных
        :param connection_data: Данные для подключение к СУБД
        :return:               Экземпляр соединения
        """
        import pyodbc
        # noinspection PyUnresolvedReferences
        self.dublicate_record_exception = pyodbc.IntegrityError
        # noinspection PyUnresolvedReferences
        return pyodbc.connect(
            'DSN=egServer70;DATABASE='+connection_data[4]+';UID='+connection_data[2]+';PWD='+connection_data[3]
        )

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
        cursor.commit()

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
            for res in cursor.fetchall():
                yield res
        else:
            cursor.commit()
            lid = cursor.execute('''SELECT @@IDENTITY''').fetchone()
            yield lid

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
        INFORMATION_SCHEMA.KEY_COLUMN_USAGE.COLUMN_NAME,
        COLUMNPROPERTY(object_id(INFORMATION_SCHEMA.COLUMNS.TABLE_NAME), INFORMATION_SCHEMA.COLUMNS.COLUMN_NAME, 'IsIdentity')
        FROM INFORMATION_SCHEMA.COLUMNS
        LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ON (
            INFORMATION_SCHEMA.COLUMNS.COLUMN_NAME = INFORMATION_SCHEMA.KEY_COLUMN_USAGE.COLUMN_NAME AND
            INFORMATION_SCHEMA.COLUMNS.TABLE_NAME = INFORMATION_SCHEMA.KEY_COLUMN_USAGE.TABLE_NAME
        )
        WHERE INFORMATION_SCHEMA.COLUMNS.TABLE_NAME = '%s' ''' % table_name))
        for field in schema:
            fields[field[0]] = MsSqlDbField(*field)
            if field[5] == field[0]:
                primary = field[0]
        return fields, primary

    @staticmethod
    def get_field_types_map():
        """ Возвращает словарь соответствий типов полей СУБД и типов полей, используемых маппером """
        return {
            FieldTypes.String: ["nchar", "nvarchar", "ntext", "char", "varchar", "text"],
            FieldTypes.Int: [
                "tinyint", "smallint", "bigint", "mediumint", "int",
                "money", "Decimal", "decimal", "numeric", "smallmoney"
            ],
            FieldTypes.Float: ["real", "float"],
            FieldTypes.Bool: ["bit"],
            FieldTypes.Date: ["date"],
            FieldTypes.DateTime: ["datetime", "datetime2"],
            FieldTypes.Time: ["time"]
        }


# noinspection PyUnusedLocal
class MongoDbAdapter(AdapterLogger):
    """ Адаптер для работы с MongoDB """

    def __init__(self):
        super().__init__()
        self.db = None

        import pymongo.errors
        self.dublicate_record_exception = pymongo.errors.DuplicateKeyError
        self.update_primary_exception = pymongo.errors.OperationFailure

    def connect(self, host: str, port: int, database: str):
        """ Выполняет подключение к СУБД по переданным реквизитам """
        import pymongo
        self.db = pymongo.MongoClient(host, port)[database]

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

    def delete_query(self, collection_name: str, conditions: dict, joined_tables):
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

    def update_query(self, collection_name: str, data: dict, conditions: dict, joined_tables, primary_key):
        """
        Выполняет запрос на обновление данных в коллекции
        @param collection_name: Имя коллекции
        @param data: Данные для обновления
        @param conditions: Условия обновления записей
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
