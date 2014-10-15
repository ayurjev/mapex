"""
Вспомогательные классы для проведения тестирования работы системы с разными СУБД
"""

from abc import ABCMeta, abstractmethod
from mapex.src.Adapters import PgSqlDbAdapter, MySqlDbAdapter, MsSqlDbAdapter, MongoDbAdapter
from mapex.src.Models import RecordModel, TableModel, EmbeddedObject, EmbeddedObjectFactory
from mapex.src.Mappers import SqlMapper, FieldTypes, NoSqlMapper
from mapex.src.Pool import Pool

import time
import re


class Profiler(object):

    def __init__(self):
        self._startTime = 0

    def __enter__(self):
        self._startTime = time.time()
        return self

    def __exit__(self, etype, value, traceback):
        pass

    def get_amount(self):
        return "Elapsed time: {:.3f} sec".format(time.time() - self._startTime)


class DbMock(object, metaclass=ABCMeta):
    """ Базовый класс для создания классов, создающих и уничтожающих инфраструктуру базы данных для тестирования """

    ident = "Unknown database"

    def __init__(self):
        self.pool = Pool(adapter=self.get_adapter(), dsn=self.get_dsn(), min_connections=2)

    def __str__(self):
        return str(self.__class__.ident)

    @abstractmethod
    def up(self):
        """ Создает нужные для проведения тестирования таблицы в базе данных """

    @abstractmethod
    def down(self):
        """ Уничтожает созданные в процессе тестирования таблицы базы данных """

    @abstractmethod
    def get_adapter(self) -> type:
        """ Возвращает тип адаптера БД """

    @abstractmethod
    def get_dsn(self) -> tuple:
        """ Возвращает DSN информацию для подключения к БД """

    @abstractmethod
    def get_new_user_instance(self, data=None, loaded_from_db=False):
        """ Возвращает новый экземпляр класса пользователя """

    @abstractmethod
    def get_new_users_collection_instance(self, boundaries=None):
        """ Возвращает новый экземпляр коллекции пользователей """

    @abstractmethod
    def get_new_users_collection_instance_with_boundaries(self):
        """ Возвращает новый экземпляр коллекции пользователей с границами """

    @abstractmethod
    def get_new_houses_collection_instance(self):
        """ Возвращает новый экземпляр коллекции домов пользователя """

    def get_houses_embedded_link(self):
        """ Возвращает экземпляр EmbeddedLink из usersTable на housesTable """

    @abstractmethod
    def get_new_account_instance(self, data=None):
        """ Возвращает новый экземпляр класса аккаунта пользователя """

    @abstractmethod
    def get_new_accounts_collection_instance(self):
        """ Возвращает новый экземпляр коллекции аккаунтов пользователей """

    @abstractmethod
    def get_new_tag_instance(self, data=None):
        """ Возвращает новый экземпляр класса тегов """

    @abstractmethod
    def get_new_tags_collection_instance(self):
        """ Возвращает новый экземпляр коллекции тегов """

    @abstractmethod
    def get_new_userstags_collection_instance(self):
        """ Возвращает новый экземпляр коллекции отношений пользователей и тегов """

    @abstractmethod
    def get_new_status_instance(self):
        """ Возвращает новый экземпляр статуса пользователя """

    @abstractmethod
    def get_new_statuses_collection_instance(self):
        """ Возвращает новый экземпляр коллекции статусов пользователя """

    @abstractmethod
    def get_new_profile_instance(self):
        """ Возвращает новый экземпляр класса профиля пользователя """

    @abstractmethod
    def get_new_profiles_collection_instance(self):
        """ Возвращает новый экземпляр коллекции статусов пользователя """

    @abstractmethod
    def get_new_noprimary_collection_instance(self):
        """ Возвращает новый экземпляр коллекции записей без первичного ключа """

    @abstractmethod
    def get_new_noprimary_instance(self, data=None):
        """ Возвращает новый экземпляр записи без первичного ключа """

    @abstractmethod
    def get_new_passports_collection_instance(self):
        """ Возвращает коллекцию паспортов и признак того, используется ли она для хранения embedded документов """

    @abstractmethod
    def get_new_passport_instance(self):
        """ Возвращает новый экземпляр класса паспорта """

    @abstractmethod
    def get_new_documents_collection_instance(self):
        """ Возвращает коллекцию документов """

    @abstractmethod
    def get_new_document_instance(self):
        """ Возвращает новый экземпляр класса документа """

    @abstractmethod
    def get_new_document_not_ai_instance(self):
        """ Возвращает экземпляр документа с неавтоинкрементным PK """

    @abstractmethod
    def get_new_documents_not_ai_instance(self):
        """ Возвращает коллекцию документов с неавтоинкрементным PK """

    @abstractmethod
    def get_new_multi_mapped_collection_instance(self):
        """ Возвращает коллекцию элементов, имеющих два разных типа отношений с одним и тем же внешним маппером """

    @abstractmethod
    def get_new_multi_mapped_collection_item(self):
        """ Возвращает запись, имеющую два разных типа отношений с одним и тем же внешним маппером """

    @abstractmethod
    def get_link_field_type(self):
        """ Возвращает тип поля маппера, используемый для связей 1-к-м """

    @abstractmethod
    def get_list_field_type(self):
        """ Возвращает тип поля маппера, используемый для связей м-к-м """

    @abstractmethod
    def get_foreign_key_field_type(self):
        """
        Возвращает тип поля маппера, используемый на стороне БД
        для хранения ссылок на записи в других коллекциях
        Например:
        get_link_field_type() -> SqlLink        get_foreign_key_field_type() -> Int
        или
        get_link_field_type() -> NoSqlLink      get_foreign_key_field_type() -> ObjectID
        """

    @abstractmethod
    def get_queries_amount(self, action):
        """
        Возвращает количество запросов, которое может быть потрачено адаптером на выполнение той или иной операции
        @param action: Тип операции
        @return: Количество истраченных адаптером запросов
        """


class SqlDbMock(DbMock):
    """ Класс для создания тестовой инфраструктуры при работе с Sql базами данных """
    def __init__(self):
        super().__init__()
        self.up_script = ""
        self.down_script = ""

    def exec_sql_from_file(self, filepath):
        """
        Выполняет набор sql-запросов из указанного файла
        @param filepath: Путь до файла с sql-командами
        """
        with open(filepath) as f:
            with self.pool as db:
                db.execute_raw(f.read())

    def up(self):
        """ Создает нужные для проведения тестирования таблицы в базе данных """
        self.exec_sql_from_file(self.up_script)
        SqlUsersMapper.pool = self.pool
        SqlUsersMapperWithBoundaries.pool = self.pool
        SqlAccountsMapper.pool = self.pool
        SqlTagsMapper.pool = self.pool
        SqlUsersTagsMapper.pool = self.pool
        SqlStatusesMapper.pool = self.pool
        SqlProfilesMapper.pool = self.pool
        SqlPassportsMapper.pool = self.pool
        SqlDocumentsMapper.pool = self.pool
        SqlDocumentsNotAiMapper.pool = self.pool
        SqlNoPrimaryMapper.pool = self.pool
        SqlMultiMappedCollectionMapper.pool = self.pool
        SqlHousesMapper.pool = self.pool

    def down(self):
        """ Уничтожает созданные в процессе тестирования таблицы базы данных """
        self.exec_sql_from_file(self.down_script)
        SqlUsersMapper.kill_instance()
        SqlUsersMapperWithBoundaries.kill_instance()
        SqlAccountsMapper.kill_instance()
        SqlTagsMapper.kill_instance()
        SqlUsersTagsMapper.kill_instance()
        SqlStatusesMapper.kill_instance()
        SqlProfilesMapper.kill_instance()
        SqlPassportsMapper.kill_instance()
        SqlDocumentsMapper.kill_instance()
        SqlDocumentsNotAiMapper.kill_instance()
        SqlNoPrimaryMapper.kill_instance()
        SqlMultiMappedCollectionMapper.kill_instance()
        SqlHousesMapper.kill_instance()

    def get_new_user_instance(self, data=None, loaded_from_db=False):
        """ Возвращает новый экземпляр класса пользователя """
        return SqlUser(data, loaded_from_db)

    def get_new_users_collection_instance(self, boundaries=None):
        """ Возвращает новый экземпляр коллекции пользователей """
        return SqlUsers(boundaries)

    def get_new_houses_collection_instance(self):
        """ Возвращает новый экземпляр коллекции домов пользователя """
        return SqlHouses()

    def get_houses_embedded_link(self):
        return SqlUsersMapper().embedded_link("house", SqlHouses)

    def get_new_users_collection_instance_with_boundaries(self):
        """ Возвращает новый экземпляр коллекции пользователей с границами """
        return SqlUsersWithBoundaries()

    def get_new_account_instance(self, data=None):
        """ Возвращает новый экземпляр класса аккаунта пользователя """
        return SqlAccount(data)

    def get_new_accounts_collection_instance(self):
        """ Возвращает новый экземпляр коллекции аккаунтов пользователей """
        return SqlAccounts()

    def get_new_tag_instance(self, data=None):
        """ Возвращает новый экземпляр класса тегов """
        return SqlTag(data)

    def get_new_tags_collection_instance(self):
        """ Возвращает новый экземпляр коллекции тегов """
        return SqlTags()

    def get_new_userstags_collection_instance(self):
        """ Возвращает новый экземпляр коллекции отношений пользователей и тегов """
        return SqlUsersTags()

    def get_new_status_instance(self):
        """ Возвращает новый экземпляр статуса пользователя """
        return SqlStatus()

    def get_new_statuses_collection_instance(self):
        """ Возвращает новый экземпляр коллекции статусов пользователя """
        return SqlStatuses()

    def get_new_profile_instance(self):
        """ Возвращает новый экземпляр класса профиля пользователя """
        return SqlProfile()

    def get_new_profiles_collection_instance(self):
        """ Возвращает новый экземпляр коллекции статусов пользователя """
        return SqlProfiles()

    def get_new_noprimary_instance(self, data=None):
        """ Возвращает новый экземпляр записи без первичного ключа """
        return SqlNoPrimaryItem(data)

    def get_new_noprimary_collection_instance(self):
        """ Возвращает новый экземпляр коллекции записей без первичного ключа """
        return SqlNoPrimaryItems()

    def get_new_passports_collection_instance(self):
        """ Возвращает коллекцию паспортов и признак того, используется ли она для хранения embedded документов """
        return SqlPassports()

    def get_new_passport_instance(self):
        """ Возвращает новый экземпляр класса паспорта """
        return SqlPassport()

    def get_new_documents_collection_instance(self):
        """ Возвращает коллекцию документов """
        return SqlDocuments()

    def get_new_document_instance(self, data=None, loaded_from_db=False):
        """ Возвращает новый экземпляр класса документа """
        return SqlDocument(data, loaded_from_db)

    def get_new_document_not_ai_instance(self, data=None, loaded_from_db=False):
        """ Возвращает новый экземпляр класса документа """
        return SqlNotAiDocument(data, loaded_from_db)

    def get_new_documents_not_ai_instance(self):
        """ Возвращает новый экземпляр класса документа """
        return SqlNotAiDocuments()

    def get_new_multi_mapped_collection_instance(self):
        """ Возвращает коллекцию элементов, имеющих два разных типа отношений с одним и тем же внешним маппером """
        return SqlMultiMappedCollection()

    def get_new_multi_mapped_collection_item(self):
        """ Возвращает запись, имеющую два разных типа отношений с одним и тем же внешним маппером """
        return SqlMultiMappedCollectionItem()

    def get_link_field_type(self):
        """ Возвращает тип поля маппера, используемый для связей 1-к-м """
        return FieldTypes.SqlLink

    def get_foreign_key_field_type(self):
        """  Возвращает тип поля, описывающее поле в БД, используемое для хранения внешних ключей """
        return FieldTypes.SqlLink

    def get_list_field_type(self):
        """ Возвращает тип поля маппера, используемый для связей м-к-м """
        return FieldTypes.SqlListWithoutRelationsTable

    def get_queries_amount(self, action):
        """
        Возвращает количество запросов, которое может быть потрачено адаптером на выполнение той или иной операции
        @param action: Тип операции
        @return: Количество истраченных адаптером запросов
        """
        return {
            "get_items": 1,
            "loading_names": 0,
            "loading_accounts": 1,
            "loading_tags": 1
        }.get(action)


class NoSqlDbMock(DbMock):
    """ Класс для создания тестовой инфраструктуры при работе с Sql базами данных """
    def up(self):
        """ Создает нужные для проведения тестирования таблицы в базе данных """
        NoSqlUsersMapper.pool = self.pool
        NoSqlUsersMapperWithBoundaries.pool = self.pool
        NoSqlAccountsMapper.pool = self.pool
        NoSqlTagsMapper.pool = self.pool
        NoSqlUsersTagsMapper.pool = self.pool
        NoSqlStatusesMapper.pool = self.pool
        NoSqlProfilesMapper.pool = self.pool
        NoSqlNoPrimaryMapper.pool = self.pool
        NoSqlPassportsMapper.pool = self.pool
        NoSqlDocumentsMapper.pool = self.pool
        NoSqlDocumentsNotAiMapper.pool = self.pool
        NoSqlMultiMappedCollectionMapper.pool = self.pool
        NoSqlHousesMaper.pool = self.pool

    def down(self):
        """ Уничтожает созданные в процессе тестирования таблицы базы данных """
        self.pool.db.db.drop_collection("usersTable")
        self.pool.db.db.drop_collection("accountsTable")
        self.pool.db.db.drop_collection("tagsTable")
        self.pool.db.db.drop_collection("users_tags_relations")
        self.pool.db.db.drop_collection("profilesTable")
        self.pool.db.db.drop_collection("statusesTable")
        self.pool.db.db.drop_collection("testTableFieldTypes")
        self.pool.db.db.drop_collection("tableWithoutPrimaryKey")
        self.pool.db.db.drop_collection("multiMappedTable")
        NoSqlUsersMapper.kill_instance()
        NoSqlUsersMapperWithBoundaries.kill_instance()
        NoSqlAccountsMapper.kill_instance()
        NoSqlTagsMapper.kill_instance()
        NoSqlUsersTagsMapper.kill_instance()
        NoSqlStatusesMapper.kill_instance()
        NoSqlProfilesMapper.kill_instance()
        NoSqlPassportsMapper.kill_instance()
        NoSqlDocumentsMapper.kill_instance()
        NoSqlDocumentsNotAiMapper.kill_instance()
        NoSqlNoPrimaryMapper.kill_instance()
        NoSqlMultiMappedCollectionMapper.kill_instance()
        NoSqlHousesMaper.kill_instance()

    def get_new_user_instance(self, data=None, loaded_from_db=False):
        """ Возвращает новый экземпляр класса пользователя """
        return NoSqlUser(data, loaded_from_db)

    def get_new_users_collection_instance(self, boundaries=None):
        """ Возвращает новый экземпляр коллекции пользователей """
        return NoSqlUsers(boundaries)

    def get_new_houses_collection_instance(self):
        return NoSqlHouses()

    def get_houses_embedded_link(self):
        return NoSqlUsersMapper().embedded_link("house", "house", NoSqlHouses)

    def get_new_users_collection_instance_with_boundaries(self):
        """ Возвращает новый экземпляр коллекции пользователей с границами """
        return NoSqlUsersWithBoundaries()

    def get_new_account_instance(self, data=None):
        """ Возвращает новый экземпляр класса аккаунта пользователя """
        return NoSqlAccount(data)

    def get_new_accounts_collection_instance(self):
        """ Возвращает новый экземпляр коллекции аккаунтов пользователей """
        return NoSqlAccounts()

    def get_new_tag_instance(self, data=None):
        """ Возвращает новый экземпляр класса тегов """
        return NoSqlTag(data)

    def get_new_tags_collection_instance(self):
        """ Возвращает новый экземпляр коллекции тегов """
        return NoSqlTags()

    def get_new_userstags_collection_instance(self):
        """ Возвращает новый экземпляр коллекции отношений пользователей и тегов """
        return False

    def get_new_status_instance(self):
        """ Возвращает новый экземпляр статуса пользователя """
        return NoSqlStatus()

    def get_new_statuses_collection_instance(self):
        """ Возвращает новый экземпляр коллекции статусов пользователя """
        return NoSqlStatuses()

    def get_new_profile_instance(self):
        """ Возвращает новый экземпляр класса профиля пользователя """
        return NoSqlProfile()

    def get_new_profiles_collection_instance(self):
        """ Возвращает новый экземпляр коллекции статусов пользователя """
        return NoSqlProfiles()

    def get_new_noprimary_instance(self, data=None):
        """ Возвращает новый экземпляр записи без первичного ключа """
        return NoSqlNoPrimaryItem(data)

    def get_new_noprimary_collection_instance(self):
        """ Возвращает новый экземпляр коллекции записей без первичного ключа """
        return NoSqlNoPrimaryItems()

    def get_new_passports_collection_instance(self):
        """ Возвращает коллекцию паспортов """
        return False

    def get_new_passport_instance(self):
        """ Возвращает новый экземпляр класса паспорта """
        return NoSqlPassport()

    def get_new_documents_collection_instance(self):
        """ Возвращает коллекцию документов """
        return False

    def get_new_document_instance(self):
        """ Возвращает новый экземпляр класса документа """
        return NoSqlDocument()

    def get_new_document_not_ai_instance(self):
        """ Возвращает документ с неавтоинкрементным PK """
        return NoSqlNotAiDocument()

    def get_new_documents_not_ai_instance(self):
        """ Возвращает коллекцию документов с неавтоинкрементным PK """
        return False

    def get_new_multi_mapped_collection_instance(self):
        """ Возвращает коллекцию элементов, имеющих два разных типа отношений с одним и тем же внешним маппером """
        return NoSqlMultiMappedCollection()

    def get_new_multi_mapped_collection_item(self):
        """ Возвращает запись, имеющую два разных типа отношений с одним и тем же внешним маппером """
        return NoSqlMultiMappedCollectionItem()

    def get_link_field_type(self):
        """ Возвращает тип поля маппера, используемый для связей 1-к-м """
        return FieldTypes.NoSqlLink

    def get_foreign_key_field_type(self):
        """  Возвращает тип поля, описывающее поле в БД, используемое для хранения внешних ключей """
        return FieldTypes.NoSqlObjectID

    def get_list_field_type(self):
        """ Возвращает тип поля маппера, используемый для связей м-к-м """
        return FieldTypes.NoSqlReversedList

    def get_queries_amount(self, action):
        """
        Возвращает количество запросов, которое может быть потрачено адаптером на выполнение той или иной операции
        @param action: Тип операции
        @return: Количество истраченных адаптером запросов
        """
        return {
            "get_items": 3,
            "loading_names": 0,
            "loading_accounts": 1,
            "loading_tags": 1
        }.get(action)


class PgDbMock(SqlDbMock):
    """ Класс для создания тестовой инфраструктуры при работе с PostgreSql """
    ident = "PostgreSQL"

    def get_adapter(self) -> type:
        """ Возвращает тип адаптера БД """
        return PgSqlDbAdapter

    def get_dsn(self) -> tuple:
        """ Возвращает DSN информацию для подключения к БД """
        return "localhost", 5432, "postgres", "z9czda5v", "postgres"

    def __init__(self):
        super().__init__()
        self.up_script = "framework/pg-up.sql"
        self.down_script = "framework/pg-down.sql"


class MyDbMock(SqlDbMock):
    """ Класс для создания тестовой инфраструктуры при работе с MySql """
    ident = "MySQL"

    def get_adapter(self) -> type:
        """ Возвращает тип адаптера БД """
        return MySqlDbAdapter

    def get_dsn(self) -> tuple:
        """ Возвращает DSN информацию для подключения к БД """
        return "localhost", 3306, "unittests", "", "unittests"

    def __init__(self):
        super().__init__()
        self.up_script = "framework/my-up.sql"
        self.down_script = "framework/my-down.sql"

class MyDbMock2(MyDbMock):
    def get_dsn(self) -> tuple:
        """ Возвращает DSN информацию для подключения к БД """
        return "localhost", 3306, "unittests", "", "unittests2"


class MsDbMock(SqlDbMock):
    """ Класс для создания тестовой инфраструктуры при работе с MsSql """
    ident = "MsSQL"

    def get_adapter(self) -> type:
        """ Возвращает тип адаптера БД """
        return MsSqlDbAdapter

    def get_dsn(self) -> tuple:
        """ Возвращает DSN информацию для подключения к БД """
        return "egServer70", "ka_user", "NHxq98S72vVSn", "orm_db"

    def __init__(self):
        super().__init__()
        self.up_script = "framework/ms-up.sql"
        self.down_script = "framework/ms-down.sql"


class MongoDbMock(NoSqlDbMock):
    """ Класс для создания тестовой инфраструктуры при работе с Mongodb """
    ident = "MongoDB"

    def get_adapter(self) -> type:
        """ Возвращает тип адаптера БД """
        return MongoDbAdapter

    def get_dsn(self) -> tuple:
        """ Возвращает DSN информацию для подключения к БД """
        return "localhost", 27017, "test"


mysql_mock, pgsql_mock, mongo_mock = MyDbMock(), PgDbMock(), MongoDbMock()
mysql_mock2 = MyDbMock2()
#mssql_mock = MsDbMock()

def for_all_dbms(test_function):
    """
    Декоратор вызывающий декорируемую функцию для всех известных СУБД
    @param test_function: Функция-тест
    """
    test_doc = re.sub("  +", "", test_function.__doc__).strip()

    # noinspection PyDocstring
    def wrapped(*args, **kwargs):
        print()
        print("%s..." % test_doc)
        for test_framework in [mysql_mock, pgsql_mock, mongo_mock]:
            test_framework.up()
            try:
                with Profiler() as p:
                    test_function(*args, dbms_fw=test_framework, **kwargs)
                    result = "%s:%sOK (%s)" % (test_framework, " "*(15 - len(str(test_framework))), p.get_amount())
                    print(result)
            finally:
                test_framework.down()
    return wrapped


########################################### Основная тестовая коллекция Users #########################################
class CustomProperty(EmbeddedObject):
    def __init__(self, value):
        self.value = value

    def get_value(self):
        return self.value

    @staticmethod
    def get_value_type():
        return int


class CustomPropertyPositive(CustomProperty):
    pass


class CustomPropertyNegative(CustomProperty):
    pass


class CustomPropertyFactory(EmbeddedObjectFactory):
    @classmethod
    def get_instance_base_type(cls):
        return CustomProperty

    @classmethod
    def get_instance(cls, value):
        if value > 0:
            return CustomPropertyPositive(value)
        else:
            return CustomPropertyNegative(value)


class CustomPropertyWithNoneFactory(EmbeddedObjectFactory):
    """ Фабрика для значений None, 1, 2 """
    # noinspection PyDocstring
    class BaseCustomType(EmbeddedObject):
        value = None

        @staticmethod
        def get_value_type():
            return int

        def get_value(self):
            return self.value

    # noinspection PyDocstring
    class CustomType1(BaseCustomType):
        value = 1

    # noinspection PyDocstring
    class CustomType2(BaseCustomType):
        value = 2


class CustomPropertyWithoutNoneFactory(EmbeddedObjectFactory):
    """ Фабрика для значений 1 и 2 """
    # noinspection PyDocstring
    class BaseCustomType(object):
        value = None

        @staticmethod
        def get_value_type():
            return int

        def get_value(self):
            return self.value

    # noinspection PyDocstring
    class CustomType1(BaseCustomType, EmbeddedObject):
        value = 1

    # noinspection PyDocstring
    class CustomType2(BaseCustomType, EmbeddedObject):
        value = 2




class SqlUsersMapper(SqlMapper):
    """ Тестовый маппер для класса Users """
    def bind(self):
        """ Настроим маппер """
        self.set_new_item(SqlUser)
        self.set_new_collection(SqlUsers)
        self.set_collection_name("usersTable")
        self.set_map([
            self.int("uid", "ID"),
            self.str("name", "Name"),
            self.int("age", "IntegerField"),
            self.bool("is_system", "isSystem"),
            self.float("latitude", "xCoord"),
            self.date("register_date", "DateField"),
            self.time("register_time", "TimeField"),
            self.datetime("register_datetime", "DateTimeField"),
            self.link("account", "AccountID", collection=SqlAccounts),
            self.list("tags", collection=SqlTags, rel_mapper=SqlUsersTagsMapper),
            self.reversed_link("profile", collection=SqlProfiles),
            self.reversed_list("statuses", collection=SqlStatuses),
            self.embedded_link("passport", collection=SqlPassports),
            self.embedded_list("documents", collection=SqlDocuments),
            self.embedded_list("documents_not_ai", collection=SqlNotAiDocuments),
            self.embedded_object("custom_property_obj", "CustomPropertyValue", model=CustomProperty),
        ])


class SqlUsersMapperWithBoundaries(SqlMapper):

    def bind(self):
        """ Настроим маппер """
        self.set_new_item(SqlUserWithBoundaries)
        self.set_new_collection(SqlUsersWithBoundaries)
        self.set_boundaries({"name": ("match", "a*")})
        self.set_collection_name("usersTable")
        self.set_map([
            self.int("uid", "ID"),
            self.str("name", "Name"),
            self.int("age", "IntegerField"),
            self.bool("is_system", "isSystem"),
            self.float("latitude", "xCoord"),
            self.date("register_date", "DateField"),
            self.time("register_time", "TimeField"),
            self.datetime("register_datetime", "DateTimeField")
        ])


class NoSqlUsersMapper(NoSqlMapper):
    """ Тестовый маппер для класса Users """
    def bind(self):
        """ Настроим маппер """
        self.set_new_item(NoSqlUser)
        self.set_new_collection(NoSqlUsers)
        self.set_collection_name("usersTable")
        self.set_map([
            self.object_id("uid", "_id"),
            self.str("name", "Name"),
            self.int("age", "IntegerField"),
            self.bool("is_system", "isSystem"),
            self.float("latitude", "xCoord"),
            self.date("register_date", "DateField"),
            self.time("register_time", "TimeField"),
            self.datetime("register_datetime", "DateTimeField"),
            self.link("account", "AccountID", collection=NoSqlAccounts),
            self.list("tags", "tagsIDS", collection=NoSqlTags),
            self.reversed_link("profile", collection=NoSqlProfiles),
            self.reversed_list("statuses", collection=NoSqlStatuses),
            self.embedded_link("passport", "Passport", collection=NoSqlPassports),
            self.embedded_list("documents", "Documents", collection=NoSqlDocuments),
            self.embedded_list("documents_not_ai", "Documentsnotai", collection=NoSqlNotAiDocuments),
            self.embedded_object("custom_property_obj", "CustomPropertyValue", model=CustomProperty)
        ])


class NoSqlUsersMapperWithBoundaries(NoSqlMapper):
    def bind(self):
        """ Настроим маппер """
        self.set_new_item(NoSqlUserWithBoundaries)
        self.set_new_collection(NoSqlUsersWithBoundaries)
        self.set_boundaries({"name": ("match", "a*")})
        self.set_collection_name("usersTable")
        self.set_map([
            self.object_id("uid", "_id"),
            self.str("name", "Name"),
            self.int("age", "IntegerField"),
            self.bool("is_system", "isSystem"),
            self.float("latitude", "xCoord"),
            self.date("register_date", "DateField"),
            self.time("register_time", "TimeField"),
            self.datetime("register_datetime", "DateTimeField")
        ])


# noinspection PyDocstring
class SqlUsers(TableModel):
    mapper = SqlUsersMapper


class SqlUsersWithBoundaries(TableModel):
    mapper = SqlUsersMapperWithBoundaries


# noinspection PyDocstring
class SqlUser(RecordModel):
    mapper = SqlUsersMapper

    def validate(self):
        if self.age == 42:
            raise Exception("Свойство age не может быть равно 42")


class SqlUserWithBoundaries(SqlUser):
    mapper = SqlUsersMapperWithBoundaries


# noinspection PyDocstring
class NoSqlUsers(TableModel):
    mapper = NoSqlUsersMapper


class NoSqlUsersWithBoundaries(TableModel):
    mapper = NoSqlUsersMapperWithBoundaries


# noinspection PyDocstring
class NoSqlUser(SqlUser):
    mapper = NoSqlUsersMapper


class NoSqlUserWithBoundaries(NoSqlUser):
    mapper = NoSqlUsersMapperWithBoundaries


class SqlHousesMapper(SqlMapper):
    def bind(self):
        self.set_new_item(SqlHouse)
        self.set_new_collection(SqlHouses)
        self.set_collection_name("housesTable")
        self.set_map([
            self.int("owner", "userID"),
            self.str("address", "address"),
        ])


class SqlHouse(RecordModel):
    mapper = SqlHousesMapper


class SqlHouses(TableModel):
    mapper = SqlHousesMapper


class NoSqlHousesMaper(NoSqlMapper):
    def bind(self):
        self.set_new_item(NoSqlHouse)
        self.set_new_collection(NoSqlHouses)
        self.set_collection_name("housesTable")
        self.set_map([
            self.object_id("owner", "_id"),
            self.str("address", "address"),
        ])


class NoSqlHouse(RecordModel):
    mapper = NoSqlHousesMaper


class NoSqlHouses(TableModel):
    mapper = NoSqlHousesMaper

############################################ Коллекция аккаунтов (Связь 1-к-м) ########################################
# noinspection PyDocstring
class SqlAccountsMapper(SqlMapper):
    def bind(self):
        self.set_new_item(SqlAccount)
        self.set_new_collection(SqlAccounts)
        self.set_collection_name("accountsTable")
        self.set_map([
            self.int("id", "AccountID"),
            self.str("email", "EmailField"),
            self.str("phone", "PhoneField"),
            self.link("profile", "ProfileID", collection=SqlProfiles)
        ])


# noinspection PyDocstring
class NoSqlAccountsMapper(NoSqlMapper):

    def bind(self):
        self.set_new_item(NoSqlAccount)
        self.set_new_collection(NoSqlAccounts)
        self.set_collection_name("accountsTable")
        self.set_map([
            self.object_id("id", "_id"),
            self.str("email", "EmailField"),
            self.str("phone", "PhoneField"),
            self.link("profile", "ProfileID", collection=NoSqlProfiles)
        ])


# noinspection PyDocstring
class SqlAccounts(TableModel):
    mapper = SqlAccountsMapper


# noinspection PyDocstring
class SqlAccount(RecordModel):
    mapper = SqlAccountsMapper


# noinspection PyDocstring
class NoSqlAccounts(TableModel):
    mapper = NoSqlAccountsMapper


# noinspection PyDocstring
class NoSqlAccount(RecordModel):
    mapper = NoSqlAccountsMapper


######################################## Коллекция тегов (Связь м-к-м) ################################################
# noinspection PyDocstring
class SqlTagsMapper(SqlMapper):
    def bind(self):
        self.set_collection_name("tagsTable")
        self.set_new_item(SqlTag)
        self.set_new_collection(SqlTags)
        self.set_map([
            self.int("id", "TagID"),
            self.str("name", "TagName"),
            self.int("weight", "TagWeight")
        ])


# noinspection PyDocstring
class SqlUsersTagsMapper(SqlMapper):
    def bind(self):
        self.set_collection_name("users_tags_relations")
        self.set_new_item(SqlUsersTagsItem)
        self.set_new_collection(SqlUsersTags)
        self.set_map([
            self.int("id", "ID"),
            self.link("user", "userID", collection=SqlUsers),
            self.link("tag", "tagID", collection=SqlTags)
        ])


# noinspection PyDocstring
class NoSqlTagsMapper(NoSqlMapper):
    def bind(self):
        self.set_collection_name("tagsTable")
        self.set_new_item(NoSqlTag)
        self.set_new_collection(NoSqlTags)
        self.set_map([
            self.object_id("id", "_id"),
            self.str("name", "TagName"),
            self.int("weight", "TagWeight")
        ])


# noinspection PyDocstring
class NoSqlUsersTagsMapper(NoSqlMapper):
    def bind(self):
        self.set_collection_name("users_tags_relations")
        self.set_new_item(NoSqlUsersTagsItem)
        self.set_new_collection(NoSqlUsersTags)
        self.set_map([
            self.object_id("id", "_id"),
            self.link("user", "userID", collection=NoSqlUsers),
            self.link("tag", "tagID", collection=NoSqlTags)
        ])


# noinspection PyDocstring
class SqlTags(TableModel):
    mapper = SqlTagsMapper


# noinspection PyDocstring
class SqlTag(RecordModel):
    mapper = SqlTagsMapper


# noinspection PyDocstring
class SqlUsersTags(TableModel):
    mapper = SqlUsersTagsMapper


# noinspection PyDocstring
class SqlUsersTagsItem(RecordModel):
    mapper = SqlUsersTagsMapper


# noinspection PyDocstring
class NoSqlTags(TableModel):
    mapper = NoSqlTagsMapper


# noinspection PyDocstring
class NoSqlTag(RecordModel):
    mapper = NoSqlTagsMapper


# noinspection PyDocstring
class NoSqlUsersTags(TableModel):
    mapper = NoSqlUsersTagsMapper


# noinspection PyDocstring
class NoSqlUsersTagsItem(RecordModel):
    mapper = NoSqlUsersTagsMapper


######################################### Коллекция статусов (Связь м-к-1 инверт.) ####################################
# noinspection PyDocstring
class SqlStatusesMapper(SqlMapper):
    def bind(self):
        self.set_collection_name("statusesTable")
        self.set_new_item(SqlStatus)
        self.set_new_collection(SqlStatuses)
        self.set_map([
            self.int("id", "ID"),
            self.str("name", "StatusName"),
            self.int("weight", "StatusWeight"),
            self.link("user", "userID", collection=SqlUsers)
        ])


# noinspection PyDocstring
class NoSqlStatusesMapper(NoSqlMapper):
    def bind(self):
        self.set_collection_name("statusesTable")
        self.set_new_item(NoSqlStatus)
        self.set_new_collection(NoSqlStatuses)
        self.set_map([
            self.object_id("id", "_id"),
            self.str("name", "StatusName"),
            self.int("weight", "StatusWeight"),
            self.link("user", "userID", collection=NoSqlUsers)
        ])


# noinspection PyDocstring
class NoSqlStatuses(TableModel):
    mapper = NoSqlStatusesMapper


# noinspection PyDocstring
class NoSqlStatus(RecordModel):
    mapper = NoSqlStatusesMapper


# noinspection PyDocstring
class SqlStatuses(TableModel):
    mapper = SqlStatusesMapper


# noinspection PyDocstring
class SqlStatus(RecordModel):
    mapper = SqlStatusesMapper


########################################## Коллекция профилей (Связь 1-к-1 инверт.) ###################################
# noinspection PyDocstring
class SqlProfilesMapper(SqlMapper):
    def bind(self):
        self.set_collection_name("profilesTable")
        self.set_new_item(SqlProfile)
        self.set_new_collection(SqlProfiles)
        self.set_map([
            self.int("id", "ID"),
            self.str("avatar", "Avatar"),
            self.int("likes", "LikesCount"),
            self.link("user", "userID", collection=SqlUsers)
        ])


# noinspection PyDocstring
class NoSqlProfilesMapper(NoSqlMapper):
    def bind(self):
        self.set_collection_name("profilesTable")
        self.set_new_item(NoSqlProfile)
        self.set_new_collection(NoSqlProfiles)
        self.set_map([
            self.object_id("id", "_id"),
            self.str("avatar", "Avatar"),
            self.int("likes", "LikesCount"),
            self.link("user", "userID", collection=NoSqlUsers)
        ])


# noinspection PyDocstring
class NoSqlProfiles(TableModel):
    mapper = NoSqlProfilesMapper


# noinspection PyDocstring
class NoSqlProfile(RecordModel):
    mapper = NoSqlProfilesMapper


# noinspection PyDocstring
class SqlProfiles(TableModel):
    mapper = SqlProfilesMapper


# noinspection PyDocstring
class SqlProfile(RecordModel):
    mapper = SqlProfilesMapper


######################################### Коллекция паспортов (Embedded links) ####################################

# noinspection PyDocstring
class SqlPassportsMapper(SqlMapper):
    def bind(self):
        self.set_collection_name("passportsTable")
        self.set_new_item(SqlPassport)
        self.set_new_collection(SqlPassports)
        self.set_map([
            self.int("id", "ID"),
            self.int("series", "Series"),
            self.int("number", "Number"),
            self.link("user", "userID", collection=SqlUsers)
        ])


# noinspection PyDocstring
class NoSqlPassportsMapper(NoSqlMapper):
    def bind(self):
        self.set_collection_name("passportsTable")
        self.set_new_item(NoSqlPassport)
        self.set_new_collection(NoSqlPassports)
        self.set_map([
            self.int("series", "Series"),
            self.int("number", "Number"),
        ])


# noinspection PyDocstring
class SqlPassports(TableModel):
    mapper = SqlPassportsMapper


# noinspection PyDocstring
class SqlPassport(RecordModel):
    mapper = SqlPassportsMapper

    invalid = False

    def validate(self):
        if self.invalid:
            raise Exception("Passport is invalid")

# noinspection PyDocstring
class NoSqlPassports(TableModel):
    mapper = NoSqlPassportsMapper


# noinspection PyDocstring
class NoSqlPassport(RecordModel):
    mapper = NoSqlPassportsMapper

    invalid = False

    def validate(self):
        if self.invalid:
            raise Exception("Passport is invalid")

######################################### Коллекция документов (Embedded lists) ####################################

# noinspection PyDocstring
class SqlDocumentsMapper(SqlMapper):
    def bind(self):
        self.set_collection_name("documentsTable")
        self.set_new_item(SqlDocument)
        self.set_new_collection(SqlDocuments)
        self.set_map([
            self.int("id", "ID"),
            self.int("series", "Series"),
            self.int("number", "Number"),
            self.link("user", "userID", collection=SqlUsers)
        ])


# noinspection PyDocstring
class NoSqlDocumentsMapper(NoSqlMapper):
    def bind(self):
        self.set_collection_name("DocumentsTable")
        self.set_new_item(NoSqlDocument)
        self.set_new_collection(NoSqlDocuments)
        self.set_map([
            self.int("series", "Series"),
            self.int("number", "Number"),
        ])


# noinspection PyDocstring
class SqlDocuments(TableModel):
    mapper = SqlDocumentsMapper


# noinspection PyDocstring
class SqlDocument(RecordModel):
    mapper = SqlDocumentsMapper


# noinspection PyDocstring
class NoSqlDocuments(TableModel):
    mapper = NoSqlDocumentsMapper


# noinspection PyDocstring
class NoSqlDocument(RecordModel):
    mapper = NoSqlDocumentsMapper

################## Коллекция документов у которых первычный ключ не автоинкрементный (Embedded lists) ##################


# noinspection PyDocstring
class SqlDocumentsNotAiMapper(SqlMapper):
    def bind(self):
        self.set_collection_name("documentsWithoutAutoincrementTable")
        self.set_new_item(SqlNotAiDocument)
        self.set_new_collection(SqlNotAiDocuments)
        self.set_map([
            self.int("series", "Series"),
            self.int("number", "Number"),
            self.link("user", "userID", collection=SqlUsers)
        ])


# noinspection PyDocstring
class NoSqlDocumentsNotAiMapper(SqlMapper):
    def bind(self):
        self.set_collection_name("documentsWithoutAutoincrementTable")
        self.set_new_item(NoSqlNotAiDocument)
        self.set_new_collection(NoSqlNotAiDocuments)
        self.set_map([
            self.int("series", "Series"),
            self.int("number", "Number"),
            self.link("user", "userID", collection=NoSqlUsers)
        ])


# noinspection PyDocstring
class SqlNotAiDocuments(TableModel):
    mapper = SqlDocumentsNotAiMapper


# noinspection PyDocstring
class SqlNotAiDocument(RecordModel):
    mapper = SqlDocumentsNotAiMapper


# noinspection PyDocstring
class NoSqlNotAiDocuments(TableModel):
    mapper = NoSqlDocumentsNotAiMapper


# noinspection PyDocstring
class NoSqlNotAiDocument(RecordModel):
    mapper = NoSqlDocumentsNotAiMapper


####################################### Коллекция элементов, не имеющая первичного ключа ##############################
# noinspection PyDocstring
class SqlNoPrimaryMapper(SqlMapper):
    def bind(self):
        self.set_collection_name("tableWithoutPrimaryKey")
        self.set_new_item(SqlNoPrimaryItem)
        self.set_new_collection(SqlNoPrimaryItems)
        self.set_map([
            self.str("name", "Name"),
            self.int("value", "Value"),
            self.datetime("time", "Time"),
            self.link("user", "userID", collection=SqlUsers)
        ])


# noinspection PyDocstring
class NoSqlNoPrimaryMapper(NoSqlMapper):
    def bind(self):
        self.set_collection_name("tableWithoutPrimaryKey")
        self.set_new_item(NoSqlNoPrimaryItem)
        self.set_new_collection(NoSqlNoPrimaryItems)
        self.set_map([
            self.str("name", "Name"),
            self.int("value", "Value"),
            self.datetime("time", "Time"),
            self.link("user", "userID", collection=NoSqlUsers)
        ])


# noinspection PyDocstring
class SqlNoPrimaryItems(TableModel):
    mapper = SqlNoPrimaryMapper


# noinspection PyDocstring
class SqlNoPrimaryItem(RecordModel):
    mapper = SqlNoPrimaryMapper


# noinspection PyDocstring
class NoSqlNoPrimaryItems(TableModel):
    mapper = NoSqlNoPrimaryMapper


# noinspection PyDocstring
class NoSqlNoPrimaryItem(RecordModel):
    mapper = NoSqlNoPrimaryMapper


################## Коллекция. имеющая два разных свойства, замапленных на одну и ту же таблицу ########################

class SqlMultiMappedCollectionMapper(SqlMapper):
    def bind(self):
        self.set_collection_name("multiMappedTable")
        self.set_new_item(SqlMultiMappedCollectionItem)
        self.set_new_collection(SqlMultiMappedCollection)
        self.set_map([
            self.int("id", "ID"),
            self.str("name", "Name"),
            self.link("author", "authorID", collection=SqlUsersWithBoundaries),
            self.link("user", "userID", collection=SqlUsers)
        ])


class NoSqlMultiMappedCollectionMapper(NoSqlMapper):
    def bind(self):
        self.set_collection_name("multiMappedTable")
        self.set_new_item(NoSqlMultiMappedCollectionItem)
        self.set_new_collection(NoSqlMultiMappedCollection)
        self.set_map([
            self.object_id("id", "_id"),
            self.str("name", "Name"),
            self.link("author", "authorID", collection=NoSqlUsersWithBoundaries),
            self.link("user", "userID", collection=NoSqlUsers)
        ])


class SqlMultiMappedCollection(TableModel):
    mapper = SqlMultiMappedCollectionMapper


class SqlMultiMappedCollectionItem(RecordModel):
    mapper = SqlMultiMappedCollectionMapper


class NoSqlMultiMappedCollection(TableModel):
    mapper = NoSqlMultiMappedCollectionMapper


class NoSqlMultiMappedCollectionItem(RecordModel):
    mapper = NoSqlMultiMappedCollectionMapper


class AMapper(SqlMapper):
    pool = MyDbMock().pool

    def bind(self):
        self.set_collection_name("a")
        self.set_new_item(AModel)
        self.set_new_collection(ACollection)
        self.set_map([
            self.int("id", "id"),
            self.link("b", "b", collection=BCollection),
            self.str("name", "name")
        ])


class BMapper(SqlMapper):
    pool = MyDbMock().pool

    def bind(self):
        self.set_collection_name("b")
        self.set_new_item(BModel)
        self.set_new_collection(BCollection)
        self.set_map([
            self.int("id", "id"),
            self.link("c", "c", collection=CCollection),
            self.str("name", "name")
        ])


class CMapper(SqlMapper):
    pool = MyDbMock().pool

    def bind(self):
        self.set_collection_name("c")
        self.set_new_item(CModel)
        self.set_new_collection(CCollection)
        self.set_map([
            self.int("id", "id"),
            self.str("name", "name")
        ])


class AModel(RecordModel):
    mapper = AMapper


class ACollection(TableModel):
    mapper = AMapper


class BModel(RecordModel):
    mapper = BMapper


class BCollection(TableModel):
    mapper = BMapper


class CModel(RecordModel):
    mapper = CMapper


class CCollection(TableModel):
    mapper = CMapper
