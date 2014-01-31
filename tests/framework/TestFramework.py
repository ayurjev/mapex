"""
Вспомогательные классы для проведения тестирования работы системы с разными СУБД
"""

from abc import ABCMeta, abstractmethod
from z9.Mapex.dbms.Adapters import PgSqlDbAdapter, MySqlDbAdapter, MsSqlDbAdapter, MongoDbAdapter
from z9.Mapex.core.Models import RecordModel, TableModel
from z9.Mapex.core.Mappers import SqlMapper, FieldTypes, NoSqlMapper

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
        for test_framework in [PgDbMock(), MongoDbMock()]:
            test_framework.up()
            try:
                with Profiler() as p:
                    test_function(*args, dbms_fw=test_framework, **kwargs)
                    result = "%s:%sOK (%s)" % (test_framework, " "*(15 - len(str(test_framework))), p.get_amount())
                    print(result)
            finally:
                test_framework.down()
    return wrapped


class DbMock(object, metaclass=ABCMeta):
    """ Базовый класс для создания классов, создающих и уничтожающих инфраструктуру базы данных для тестирования """

    ident = "Unknown database"

    def __init__(self):
        self.db = None

    def __str__(self):
        return str(self.__class__.ident)

    @abstractmethod
    def up(self):
        """ Создает нужные для проведения тестирования таблицы в базе данных """

    @abstractmethod
    def down(self):
        """ Уничтожает созданные в процессе тестирования таблицы базы данных """

    @abstractmethod
    def get_new_user_instance(self):
        """ Возвращает новый экземпляр класса пользователя """

    @abstractmethod
    def get_new_users_collection_instance(self):
        """ Возвращает новый экземпляр коллекции пользователей """

    @abstractmethod
    def get_new_account_instance(self):
        """ Возвращает новый экземпляр класса аккаунта пользователя """

    @abstractmethod
    def get_new_accounts_collection_instance(self):
        """ Возвращает новый экземпляр коллекции аккаунтов пользователей """

    @abstractmethod
    def get_new_tag_instance(self):
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
    def get_new_noprimary_instance(self):
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
        f = open(filepath)
        script = "".join(f.readlines())
        f.close()
        self.db.execute_raw(script)

    def up(self):
        """ Создает нужные для проведения тестирования таблицы в базе данных """
        self.exec_sql_from_file(self.up_script)
        SqlUsersMapper.db = self.db
        SqlAccountsMapper.db = self.db
        SqlTagsMapper.db = self.db
        SqlUsersTagsMapper.db = self.db
        SqlStatusesMapper.db = self.db
        SqlProfilesMapper.db = self.db
        SqlPassportsMapper.db = self.db
        SqlDocumentsMapper.db = self.db
        SqlNoPrimaryMapper.db = self.db

    def down(self):
        """ Уничтожает созданные в процессе тестирования таблицы базы данных """
        self.exec_sql_from_file(self.down_script)
        SqlUsersMapper.kill_instance()
        SqlAccountsMapper.kill_instance()
        SqlTagsMapper.kill_instance()
        SqlUsersTagsMapper.kill_instance()
        SqlStatusesMapper.kill_instance()
        SqlProfilesMapper.kill_instance()
        SqlPassportsMapper.kill_instance()
        SqlDocumentsMapper.kill_instance()
        SqlNoPrimaryMapper.kill_instance()

    def get_new_user_instance(self):
        """ Возвращает новый экземпляр класса пользователя """
        return SqlUser()

    def get_new_users_collection_instance(self):
        """ Возвращает новый экземпляр коллекции пользователей """
        return SqlUsers()

    def get_new_account_instance(self):
        """ Возвращает новый экземпляр класса аккаунта пользователя """
        return SqlAccount()

    def get_new_accounts_collection_instance(self):
        """ Возвращает новый экземпляр коллекции аккаунтов пользователей """
        return SqlAccounts()

    def get_new_tag_instance(self):
        """ Возвращает новый экземпляр класса тегов """
        return SqlTag()

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

    def get_new_noprimary_instance(self):
        """ Возвращает новый экземпляр записи без первичного ключа """
        return SqlNoPrimaryItem()

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

    def get_new_document_instance(self):
        """ Возвращает новый экземпляр класса документа """
        return SqlDocument()

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
        NoSqlUsersMapper.db = self.db
        NoSqlAccountsMapper.db = self.db
        NoSqlTagsMapper.db = self.db
        NoSqlUsersTagsMapper.db = self.db
        NoSqlStatusesMapper.db = self.db
        NoSqlProfilesMapper.db = self.db
        NoSqlNoPrimaryMapper.db = self.db
        NoSqlPassportsMapper.db = self.db
        NoSqlDocumentsMapper.db = self.db

    def down(self):
        """ Уничтожает созданные в процессе тестирования таблицы базы данных """
        self.db.db.drop_collection("usersTable")
        self.db.db.drop_collection("accountsTable")
        self.db.db.drop_collection("tagsTable")
        self.db.db.drop_collection("users_tags_relations")
        self.db.db.drop_collection("profilesTable")
        self.db.db.drop_collection("statusesTable")
        self.db.db.drop_collection("testTableFieldTypes")
        self.db.db.drop_collection("tableWithoutPrimaryKey")
        NoSqlUsersMapper.kill_instance()
        NoSqlAccountsMapper.kill_instance()
        NoSqlTagsMapper.kill_instance()
        NoSqlUsersTagsMapper.kill_instance()
        NoSqlStatusesMapper.kill_instance()
        NoSqlProfilesMapper.kill_instance()
        NoSqlPassportsMapper.kill_instance()
        NoSqlDocumentsMapper.kill_instance()
        NoSqlNoPrimaryMapper.kill_instance()

    def get_new_user_instance(self):
        """ Возвращает новый экземпляр класса пользователя """
        return NoSqlUser()

    def get_new_users_collection_instance(self):
        """ Возвращает новый экземпляр коллекции пользователей """
        return NoSqlUsers()

    def get_new_account_instance(self):
        """ Возвращает новый экземпляр класса аккаунта пользователя """
        return NoSqlAccount()

    def get_new_accounts_collection_instance(self):
        """ Возвращает новый экземпляр коллекции аккаунтов пользователей """
        return NoSqlAccounts()

    def get_new_tag_instance(self):
        """ Возвращает новый экземпляр класса тегов """
        return NoSqlTag()

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

    def get_new_noprimary_instance(self):
        """ Возвращает новый экземпляр записи без первичного ключа """
        return NoSqlNoPrimaryItem()

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

    def __init__(self):
        super().__init__()
        self.db = PgSqlDbAdapter()
        self.db.connect(("localhost", 5432, "postgres", "z9czda5v", "postgres"))
        self.up_script = "framework/pg-up.sql"
        self.down_script = "framework/pg-down.sql"


class MyDbMock(SqlDbMock):
    """ Класс для создания тестовой инфраструктуры при работе с MySql """

    ident = "MySQL"

    def __init__(self):
        super().__init__()
        self.db = MySqlDbAdapter()
        self.db.connect(("localhost", 3306, "root", "z9czda5v", "test"))
        self.up_script = "framework/my-up.sql"
        self.down_script = "framework/my-down.sql"


class MsDbMock(SqlDbMock):
    """ Класс для создания тестовой инфраструктуры при работе с MsSql """

    ident = "MsSQL"

    def __init__(self):
        super().__init__()
        self.db = MsSqlDbAdapter()
        self.db.connect(("MSSQL_HOST", 1433, "ka_user", "NHxq98S72vVSn", "orm_db"))
        self.up_script = "framework/ms-up.sql"
        self.down_script = "framework/ms-down.sql"


class MongoDbMock(NoSqlDbMock):

    ident = "MongoDB"

    def __init__(self):
        super().__init__()
        self.db = MongoDbAdapter()
        self.db.connect("localhost", 27017, "test")


########################################### Основная тестовая коллекция Users #########################################
class SqlUsersMapper(SqlMapper):
    """ Тестовый маппер для класса Users """
    def bind(self):
        """ Настроим маппер """
        self.set_new_item(SqlUser)
        self.set_new_collection(SqlUsers)
        self.attach(table_name="usersTable")
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
            self.embedded_list("documents", collection=SqlDocuments)
        ])


class NoSqlUsersMapper(NoSqlMapper):
    """ Тестовый маппер для класса Users """
    def bind(self):
        """ Настроим маппер """
        self.set_new_item(NoSqlUser)
        self.set_new_collection(NoSqlUsers)
        self.attach(table_name="usersTable")
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
            self.embedded_list("documents", "Documents", collection=NoSqlDocuments)
        ])


# noinspection PyDocstring
class SqlUsers(TableModel):
    mapper = SqlUsersMapper


# noinspection PyDocstring
class SqlUser(RecordModel):
    mapper = SqlUsersMapper

    def validate(self):
        if self.age == 42:
            raise Exception("Свойство age не может быть равно 42")

    def stringify(self, properties=None, stringify_depth=None, additional_data=None):
        return super().stringify(properties, stringify_depth=stringify_depth, additional_data={
            "new_property": self.age * 100 if self.age is not None else None
        })


# noinspection PyDocstring
class NoSqlUsers(TableModel):
    mapper = NoSqlUsersMapper


# noinspection PyDocstring
class NoSqlUser(SqlUser):
    mapper = NoSqlUsersMapper


############################################ Коллекция аккаунтов (Связь 1-к-м) ########################################
# noinspection PyDocstring
class SqlAccountsMapper(SqlMapper):
    def bind(self):
        self.set_new_item(SqlAccount)
        self.set_new_collection(SqlAccounts)
        self.attach(table_name="accountsTable")
        self.set_map([
            self.int("id", "AccountID"),
            self.str("email", "EmailField"),
            self.str("phone", "PhoneField")
        ])


# noinspection PyDocstring
class NoSqlAccountsMapper(NoSqlMapper):
    def bind(self):
        self.set_new_item(NoSqlAccount)
        self.set_new_collection(NoSqlAccounts)
        self.attach(table_name="accountsTable")
        self.set_map([
            self.object_id("id", "_id"),
            self.str("email", "EmailField"),
            self.str("phone", "PhoneField")
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
        self.attach(table_name="tagsTable")
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
        self.attach(table_name="users_tags_relations")
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
        self.attach(table_name="tagsTable")
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
        self.attach(table_name="users_tags_relations")
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
        self.attach(table_name="statusesTable")
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
        self.attach(table_name="statusesTable")
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
        self.attach(table_name="profilesTable")
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
        self.attach(table_name="profilesTable")
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
        self.attach(table_name="passportsTable")
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
        self.attach(table_name="passportsTable")
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


# noinspection PyDocstring
class NoSqlPassports(TableModel):
    mapper = NoSqlPassportsMapper


# noinspection PyDocstring
class NoSqlPassport(RecordModel):
    mapper = NoSqlPassportsMapper


######################################### Коллекция документов (Embedded lists) ####################################

# noinspection PyDocstring
class SqlDocumentsMapper(SqlMapper):
    def bind(self):
        self.attach(table_name="documentsTable")
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
        self.attach(table_name="DocumentsTable")
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


####################################### Коллекция элементов, не имеющая первичного ключа ##############################
# noinspection PyDocstring
class SqlNoPrimaryMapper(SqlMapper):
    def bind(self):
        self.attach(table_name="tableWithoutPrimaryKey")
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
        self.attach(table_name="tableWithoutPrimaryKey")
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