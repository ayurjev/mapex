""" Модуль для работы с БД """
import re
import time
from datetime import datetime, date, time as dtime
from abc import abstractmethod, ABCMeta
from collections import OrderedDict
from collections import defaultdict

from mapex.core.Exceptions import TableModelException, TableMapperException, DublicateRecordException
from mapex.core.Models import RecordModel, TableModel, EmbeddedObject, EmbeddedObjectFactory


class Primary(object):
    """ Класс для представления первичных ключей мапперов """
    def __init__(self, mapper, name_in_db: str=None, name_in_mapper: str=None):
        """
        @param mapper: Родительский маппер
        @param name_in_db: Имя первичного ключа в базе данных
        @type name_in_db: str
        @param name_in_mapper: Имя первичного ключа в маппере
        @type name_in_mapper: str
        """
        self.defined_by_user = False
        self.db_primary_key = None
        self.primary = None
        self.mapper = mapper
        self.compound = False
        if name_in_db:
            self.db_primary_key = name_in_db
            primary_candidates = list(filter(
                lambda prop_name: self.mapper.get_property(prop_name).get_db_name() == name_in_db,
                self.mapper.get_properties()
            ))
            self.primary = primary_candidates[0] if len(primary_candidates) > 0 else None
        elif name_in_mapper:
            self.primary = name_in_mapper
            if type(name_in_mapper) is list:
                self.compound = True
            else:
                self.db_primary_key = self.mapper.get_property(name_in_mapper).get_db_name()

        self.autoincremented = False
        if self.db_primary_key and self.db_primary_key in self.mapper.db_fields.keys():
            self.autoincremented = self.mapper.db_fields.get(self.db_primary_key).autoincremented

    def db_name(self):
        """ Возвращает имя ключа в базе данных
        @return: Имя поля, являющегося первичным ключом в базе данных

        """
        return self.db_primary_key

    def name(self):
        """
        Возвращает имя поля маппера
        @return: Имя поля маппера
        @raise TableMapperException: Если первичного ключа в маппере нет

        """
        if self.primary is None:
            raise TableModelException("there is no primary key in mapper '%s'" % self.mapper)
        return self.primary

    def exists(self):
        """
        Возвращает признак того, есть ли в маппере первичный ключ
        @return: Есть первичный ключ или нет

        """
        return self.primary is not None

    def grab_value_from(self, data):
        """
        Возвращает значение первичного ключа, полученное из переданного словаря data
        @param data: Словарь с данными или некое значение
        @return: Значение первичного ключа
        @raise TableMapperException: Если переданные данные не соответствуют формату первичного ключа

        """
        if type(data) is dict:
            return {
                field: data.get(field) for field in self.primary
            } if self.compound else data.get(self.primary)
        else:
            if self.compound:
                raise TableMapperException("Invalid data for the compound primary")
            else:
                return data

    def eq_condition(self, data):
        """
        Возвращает словарь, представляющий собой первичный ключ
        и пригодный для использования в качестве условий выборки
        @param data: Данные для установки значений в поля первичного ключа или значение первичного ключа
        @return: Значение первичного ключа в виде словаря
        @raise TableMapperException: Если переданные данные не соответствуют формату первичного ключа

        """
        res = self.grab_value_from(data)
        if not self.compound:
            if type(res) is not dict:
                res = {self.name(): res}
        return res


class FieldTypes(object):
    """ Коллекция типов полей, которые могут использоваться мапперами """

    class BaseField(metaclass=ABCMeta):
        """
        Базовый класс для создания любых других типов полей
        @param mapper: Основной маппер
        @param mapper_field_name: Имя поля маппера
        @param db_field_name: Имя поля в таблице базы данных

        """
        ident = "BaseField"

        def __init__(self, mapper, mapper_field_name, **kwargs):
            self.mapper = mapper
            self.mapper_field_name = mapper_field_name
            self.db_field_name = kwargs.get("db_field_name")

        @abstractmethod
        def value_assertion(self, v) -> bool:
            """
            Проверка корректности значения
            @param v: Значение для проверки
            @return:  Корректно/Некорректно
            @rtype : bool

            """

        def get_name(self) -> str:
            """
            Возвращает имя поля маппера
            @return: Имя поля маппера
            @rtype : str

            """
            return self.mapper_field_name

        def get_db_name(self) -> str:
            """
            Возвращает имя поля маппера в базе данных
            @return:  Имя поля маппера в базе данных
            @rtype : str


            """
            return self.db_field_name

        def get_db_type(self) -> str:
            """
            Возвращает тип поля маппера в базе данных
            @return: Тип поля маппера в базе данных
            @rtype : str

            """
            return self.mapper.get_db_type(self.db_field_name)

        def get_mapper_type(self):
            """ Возвращает тип, используемый для поля на стороне маппера """
            return self.__class__

        def get_db_type_in_mapper_terms(self):
            """
            Возвращает тип поля маппера
            @return: Тип поля маппера
            """
            db_type = self.get_db_type()
            field_types = self.mapper.db.get_field_types_map()
            for mapperType in field_types:
                if db_type in field_types[mapperType]:
                    return mapperType
            return FieldTypes.Unknown
            #raise TableMapperException("Unknown database field type: %s" % db_type)

        def get_default_value(self):
            """
            Возвращает значение, которое должно принимать поле, если оно не проинициализировано
            @return: Значение для этого типа поля по умолчанию

            """
            return FieldValues.NoneValue()

        def check_value(self, value):
            """
            Проверяет соответствует ли данное значение данному полю маппера
            @param value: Значение для хранения в данном поле
            @raise TableModelException: Если значение не проходит проверку

            """
            if value and self.value_assertion(value) is False:
                raise TableModelException(
                    "\ntype of the value for field '%s' with type %s can't be %s (%s)" % (
                        self.mapper_field_name, self.get_mapper_type(), value, type(value)
                    )
                )

        def convert(self, value, direction, cache):
            """
            Конвертирует значение от формата маппера к формату базы данных или наоборот в зависимости от direction
            @param value: Значение для конвертации
            @param direction: Направление конвертации
            @param cache: Используемый кэш
            @return: Сконвертированное значение

            """
            # noinspection PyDocstring
            def try_convert(s, d):
                try:
                    return FieldTypesConverter.converters[(s, d)](value, self, cache)
                except KeyError:
                    raise TableMapperException("\ncan't convert value %s from %s to %s" % (value, s, d))

            if direction == "mapper2database":
                if isinstance(value, FieldValues.NoneValue) is False:
                    self.check_value(value)
                converted = try_convert(self.get_mapper_type().ident, self.get_db_type_in_mapper_terms().ident)
                return converted if isinstance(converted, FieldValues.NoneValue) is False else None
            else:
                converted = try_convert(self.get_db_type_in_mapper_terms().ident, self.get_mapper_type().ident)
                if isinstance(converted, FieldValues.NoneValue) is False:
                    self.check_value(converted)
                return converted

        def translate(self, name, direction):
            """
            Конвертирует обращение к полю маппера в обращение к полю таблицы или наоборот в зависимости от direction
            @param name: Обращение для конвертации
            @type name: str
            @param direction: Направление конвертации
            @type direction: str
            @return: Сконвертированное обращение
            @rtype : str

            """
            if name is None:
                return None

            if direction == "mapper2database":
                if name.find(".") > -1:
                    mapper_field_name, mapper_property = name.split(".")
                    linked_mapper = self.mapper.get_property(mapper_field_name).get_items_collection_mapper()
                    return "%s.%s" % (
                        mapper_field_name
                        if self.mapper.support_joins else self.translate(mapper_field_name, "mapper2database"),
                        linked_mapper.translate(mapper_property, "mapper2database")
                    )
                else:
                    mapper_field = self.mapper.get_property(name)
                    return mapper_field.get_db_name() if mapper_field else name
            else:
                if name.endswith("]"):
                    return re.search("\[(.+?)\]", name).group(1)

                if name.find(".") > -1:
                    table_name, field_name = name.split(".")
                    mapper_property = self.mapper.get_property(table_name)
                    return '%s.%s' % (
                        mapper_property.get_name(),
                        mapper_property.get_items_collection_mapper().translate(field_name, "database2mapper")
                    )
                return self.mapper.get_property_by_db_name(name).get_name()

    class NoSqlBaseField(BaseField):

        def __init__(self, mapper, mapper_field_name, **kwargs):
            super().__init__(mapper, mapper_field_name, **kwargs)
            self.db_field_type = kwargs.get("db_field_type", self.__class__)

        def get_db_type_in_mapper_terms(self) -> type:
            """
            Возвращает тип поля маппера
            @return: Тип поля маппера
            @rtype : type

            """
            return self.db_field_type

        @abstractmethod
        def value_assertion(self, v) -> bool:
            """
            Проверка корректности значения
            @param v: Значение для проверки
            @return:  Корректно/Некорректно
            @rtype : bool

            """

    class Int(BaseField):
        """ Класс для представления целочисленного типа поля уровня маппера """

        ident = "Int"

        def value_assertion(self, v) -> bool:
            """ Проверка корректности значения
            @param v: Значение для проверки
            @return:  Корректно/Некорректно
            @rtype : bool

            """
            return str(v).isdigit()

    class NoSqlInt(Int, NoSqlBaseField):
        pass

    class String(BaseField):
        """ Класс для представления строкового типа поля уровня маппера """

        ident = "String"

        def value_assertion(self, v) -> bool:
            """ Проверка корректности значения
            @param v: Значение для проверки
            @return:  Корректно/Некорректно
            @rtype : bool

            """
            return isinstance(v, str)

    class NoSqlString(String, NoSqlBaseField):
        pass

    class Repr(String):
        ident = "Repr"

    class Unknown(String):
        ident = "Unknown"

    class NoSqlUnknown(NoSqlString):
        ident = "Unknown"

    class NoSqlObjectID(NoSqlBaseField, BaseField):

        ident = "ObjectID"

        def value_assertion(self, v) -> bool:
            """ Проверка корректности значения
            @param v: Значение для проверки
            @return:  Корректно/Некорректно
            @rtype : bool

            """
            return True

    class EmbeddedObject(BaseField):
        """ Класс для представления кастомного типа поля уровня маппера """

        ident = "EmbeddedObject"

        def __init__(self, mapper, mapper_field_name, model, **kwargs):
            """
            @param mapper: Основной маппер, которому принадлежит поле
            @type mapper: Mapper
            @param mapper_field_name: Имя поля маппера
            @type mapper_field_name: str
            @param model: Класс привязанной модели

            """
            super().__init__(mapper, mapper_field_name, **kwargs)
            self.model = model

        def get_value_type_in_mapper_terms(self, value_type):
            return {
                int: FieldTypes.Int,
                str: FieldTypes.String,
                float: FieldTypes.Float,
                date: FieldTypes.Date,
                time: FieldTypes.Time,
                datetime: FieldTypes.DateTime,
                bool: FieldTypes.Bool
            }.get(value_type)

        def value_assertion(self, v) -> bool:
            if issubclass(self.model, EmbeddedObject):
                model = self.model
            elif issubclass(self.model, EmbeddedObjectFactory):
                model = self.model.get_instance(v.get_value())
            else:
                return False
            return isinstance(v, EmbeddedObject) and isinstance(v.get_value(), model.get_value_type())

    class NoSqlEmbeddedObject(EmbeddedObject, NoSqlBaseField):
        """ Класс для представления кастомного типа поля уровня маппера """
        def get_value_type_in_mapper_terms(self, value_type):
            return {
                int: FieldTypes.NoSqlInt,
                str: FieldTypes.NoSqlString,
                float: FieldTypes.NoSqlFloat,
                date: FieldTypes.NoSqlDate,
                time: FieldTypes.NoSqlTime,
                datetime: FieldTypes.NoSqlDateTime,
                bool: FieldTypes.NoSqlBool
            }.get(value_type)

    class Bool(BaseField):
        """ Класс для представления булевого типа поля уровня маппера """

        ident = "Bool"

        def value_assertion(self, v) -> bool:
            """ Проверка корректности значения
            @param v: Значение для проверки
            @return:  Корректно/Некорректно
            @rtype : bool

            """
            return isinstance(v, bool)

    class NoSqlBool(Bool, NoSqlBaseField):
        pass

    class Float(BaseField):
        """ Класс для представления значений маппера с плавающей точкой """

        ident = "Float"

        def value_assertion(self, v) -> bool:
            """ Проверка корректности значения
            @param v: Значение для проверки
            @return:  Корректно/Некорректно
            @rtype : bool

            """
            return isinstance(v, float) or (isinstance(v, int) and int(float(v)) == v)

    class NoSqlFloat(Float, NoSqlBaseField):
        pass

    class Date(BaseField):
        """ Класс для представления дат маппера """

        ident = "Date"

        def value_assertion(self, v) -> bool:
            """ Проверка корректности значения
            @param v: Значение для проверки
            @return:  Корректно/Некорректно
            @rtype : bool

            """
            return isinstance(v, date)

    class NoSqlDate(Date, NoSqlBaseField):
        pass

    class Time(BaseField):
        """ Класс для представления полей маппера типа "Время" """

        ident = "Time"

        def value_assertion(self, v) -> bool:
            """ Проверка корректности значения
            @param v: Значение для проверки
            @return:  Корректно/Некорректно
            @rtype : bool

            """
            return isinstance(v, dtime)

    class NoSqlTime(Time, NoSqlBaseField):
        pass

    class DateTime(BaseField):
        """ Класс для представления полей маппера типа "Дата и время" """

        ident = "DateTime"

        def value_assertion(self, v) -> bool:
            """ Проверка корректности значения
            @param v: Значение для проверки
            @return:  Корректно/Некорректно
            @rtype : bool

            """
            return isinstance(v, datetime)

    class NoSqlDateTime(DateTime, NoSqlBaseField):
        pass

    ################################# Общие типы полей со связями ##################################################

    class RelationField(BaseField):
        """ Базовый класс для всех типов полей, являющихся связями с другими таблицами """

        ident = "Rel"

        def __init__(self, mapper, mapper_field_name, **kwargs):
            """
            @param mapper: Основной маппер, которому принадлежит поле
            @type mapper: Mapper
            @param mapper_field_name: Имя поля маппера
            @type mapper_field_name: str
            @param db_field_name: Имя поля в таблице базы данных
            @type db_field_name: str
            @param field_model: Класс привязанной модели

            """
            super().__init__(mapper, mapper_field_name, **kwargs)
            item = kwargs.get("joined_collection")().get_new_item()
            self.item_class = item.__class__
            self.items_collection_mapper = item.mapper

            if item.mapper.binded:
                if item.mapper.primary.exists() is False and self.is_primary_required():
                    raise TableModelException("There is no primary key in %s" % item.mapper)

        @staticmethod
        def is_primary_required():
            return True

        def get_new_item(self):
            """
            Возвращает новый экземпляр класса значения для этого поля
            @return: Новый экземпляр класса значения для этого поля

            """
            return self.item_class()

        def get_items_collection_mapper(self):
            """ Возвращает маппер прилинкованной модели """
            return self.items_collection_mapper

        @abstractmethod
        def value_assertion(self, v) -> bool:
            """
            Проверяет переданное значение на соответствие данному типу поля
            @param v: Значение для проверки
            @return: Результат проверки

            """

    class BaseLink(RelationField):
        """ Класс для типа поля маппера, реализующего связь один-ко-многим """

        ident = "Link"

        def value_assertion(self, v):
            """ Проверяет корректность и соответствие этому типу поля значений
            @param v: Значение для проверки
            @return: Результат проверки

            """
            return isinstance(v, self.item_class) and isinstance(v, RecordModel) if v else True

    class BaseList(RelationField):

        ident = "List"

        def value_assertion(self, v):
            """
            Проверяет корректность и соответствие этому типу поля значений
            @param v: Значение для проверки
            @return: Результат проверки

            """
            return isinstance(v, list) and len(list(filter(
                lambda elem: isinstance(elem, self.item_class) is False, v
            ))) == 0

        def get_default_value(self):
            """
            Возвращает значение для этого типа поля, используемое по умолчанию
            @return: Значение по умолчанию

            """
            return FieldValues.ListValue()

    class BaseForeignCollectionList(BaseList):

        def save_items(self, items: list, main_record_obj: RecordModel):
            """
            Сохраняет все привязанные к основному объекту объекты
            @param items: Список объектов
            @type items: list
            @param main_record_obj: Основной объект
            @type main_record_obj: RecordModel

            """
            main_record_key = self.items_collection_mapper.get_property_that_is_link_for(self.mapper).get_name()
            self.items_collection_mapper.update(
                {"%s" % main_record_key: None},
                {"%s.%s" % (main_record_key, self.mapper.primary.name()): main_record_obj.get_actual_primary_value()}
            )
            for obj in filter(None, items):
                obj.__setattr__(main_record_key, main_record_obj)
                obj.save()

        def clear_dependencies_from(self, main_records_ids: list):
            """
            Очищает зависимость присоединенных записей, от тех записей основной таблицы, которые были удалены и
            перечислены в main_records_ids
            @param main_records_ids: Список значений первичных ключей, удаленных записей основной таблицы
            @type main_records_ids: list

            """
            main_record_key = self.items_collection_mapper.get_property_that_is_link_for(self.mapper).get_name()
            self.items_collection_mapper.update(
                {main_record_key: None},
                {"%s.%s" % (main_record_key, self.mapper.primary.name()): ("in", main_records_ids)}
            )

    class BaseReversedLink(BaseList):
        ident = "ReversedLink"

        def value_assertion(self, v):
            """
            Проверяет корректность и соответствие этому типу поля значений
            @param v: Значение для проверки
            @return: Результат проверки

            """
            return isinstance(v, self.item_class) and isinstance(v, RecordModel) if v else True

        def get_default_value(self):
            """
            Возвращает значение для этого типа поля, используемое по умолчанию
            @return: Значение по умолчанию

            """
            return FieldValues.NoneValue()

        def save_items(self, item: RecordModel, main_record: RecordModel):
            """
            Сохраняет все привязанные к основному объекту объекты
            @param item: Привязанная модель
            @type item: RecordModel
            @param main_record: Основной объект
            @type main_record: RecordModel

            """
            main_record_key = self.items_collection_mapper.get_property_that_is_link_for(self.mapper).get_name()
            if item:
                item.__setattr__(main_record_key, main_record)
                item.save()
            else:
                self.items_collection_mapper.update(
                    {"%s" % main_record_key: None},
                    {"%s.%s" % (main_record_key, self.mapper.primary.name()): main_record.get_actual_primary_value()}
                )

    class NoSqlRelationField(RelationField, NoSqlBaseField):

        @abstractmethod
        def value_assertion(self, v) -> bool:
            """
            Проверяет переданное значение на соответствие данному типу поля
            @param v: Значение для проверки
            @return: Результат проверки

            """

    class NoSqlBaseList(BaseList, NoSqlRelationField):
        pass

    ############################################# Sql ##############################################################

    class SqlLink(BaseLink):
        pass

    class SqlList(BaseList):
        """ Класс для типа поля маппера, реализующего связь многие-ко-многим """

        def __init__(self, mapper, mapper_field_name, **kwargs):
            super().__init__(mapper, mapper_field_name, db_field_name="",
                             **kwargs)

        @abstractmethod
        def get_relations_mapper(self):
            """
            Возвращает маппер таблицы отношений
            @return: Маппер таблицы отношений

            """

        def get_db_type(self):
            """
            Переопределяем метод получения типа поля в базе данных, так как база данных ничего об этом поле не знает
            @return: Тип поля в базе данных

            """
            return "text"

    class SqlListWithRelationsTable(SqlList, BaseForeignCollectionList):
        """ Класс для реализации отношений многие-ко-многим, использующих таблицу отношений записей """

        def __init__(self, mapper, mapper_field_name, **kwargs):
            """
            @param mapper: Маппер основной таблицы
            @type mapper: Mapper
            @param mapper_field_name: Имя поля маппера
            @type mapper_field_name: str
            @param joined_collection: Тип привязанной коллекции
            @type joined_collection: TableModel
            @param rel_mapper: Маппер таблицы отношений, с помощью которого связаны коллекции

            """
            super().__init__(mapper, mapper_field_name, **kwargs)
            self.rel_mapper = kwargs.get("rel_mapper")()
            self.db_field_name = "%s.%s[%s]" % (
                self.rel_mapper.table_name,
                self.rel_mapper.get_property_that_is_link_for(self.items_collection_mapper).get_db_name(),
                mapper_field_name
            )

        def save_items(self, items: list, main_record_obj: RecordModel):
            """
            Сохраняет все привязанные к основному объекту объекты
            @param items: Список объектов
            @type items: list
            @param main_record_obj: Основной объект
            @type main_record_obj: RecordModel

            """
            main_record_key = self.rel_mapper.get_property_that_is_link_for(self.mapper).get_name()
            second_record_key = self.rel_mapper.get_property_that_is_link_for(self.items_collection_mapper).get_name()
            self.rel_mapper.delete(
                {"%s.%s" % (main_record_key, self.mapper.primary.name()): main_record_obj.get_actual_primary_value()}
            )
            self.rel_mapper.insert([{main_record_key: main_record_obj, second_record_key: obj} for obj in items])

        def clear_dependencies_from(self, main_records_ids: list):
            """
            Очищает зависимость присоединенных записей, от тех записей основной таблицы, которые были удалены и
            перечислены в main_records_ids
            @param main_records_ids: Список значений первичных ключей, удаленных записей основной таблицы
            @type main_records_ids: list

            """
            main_record_key = self.rel_mapper.get_property_that_is_link_for(self.mapper).get_name()
            self.rel_mapper.delete(
                {"%s.%s" % (main_record_key, self.mapper.primary.name()): ("in", main_records_ids)}
            )

        def get_relations_mapper(self):
            """ Возвращает маппер таблицы отношений """
            return self.rel_mapper

    class SqlListWithoutRelationsTable(SqlList, BaseForeignCollectionList):
        """
        Класс для реализации отношений многие-к-одному, когда таблицы отношений нет,
        но некое множество записей одной таблицы ссылаются на одну запись в основной таблице
        """

        def __init__(self, mapper, mapper_field_name, **kwargs):
            """
            @param mapper: Маппер основной таблицы
            @type mapper: Mapper
            @param mapper_field_name: Имя поля маппера
            @type mapper_field_name: str
            @param joined_collection: Тип привязанной коллекции
            @type joined_collection: TableModel

            """
            super().__init__(mapper, mapper_field_name, **kwargs)
            if self.items_collection_mapper.primary.compound:
                self.db_field_name = "%s.%s" % (
                    self.items_collection_mapper.table_name,
                    "+".join([
                        "%s[%s]" % (
                            self.items_collection_mapper.translate(pf, "mapper2database"),
                            mapper_field_name
                        ) for pf in self.items_collection_mapper.primary.name()
                    ])
                )
            else:
                self.db_field_name = "%s.%s[%s]" % (
                    self.items_collection_mapper.table_name,
                    self.items_collection_mapper.translate(
                        self.items_collection_mapper.primary.name(),
                        "mapper2database"
                    ),
                    mapper_field_name
                )

        def get_relations_mapper(self):
            """ Возвращает маппер таблицы отношений - в данном случае это и есть таблица привязанных объектов """
            return self.items_collection_mapper

    class SqlReversedLink(BaseReversedLink, SqlListWithoutRelationsTable):
        """
        Класс для типа поля маппера, реализующего связь один-к-одному,
        когда в основной таблице нет ссылки на привязанную запись, но в некой другой таблице какая-то запись
        ссылается на запись в основной таблице.
        Вообще, это частный случай отношения многие-к-одному, реализуемому классом List,
        сконфигурированным без указания таблицы отношений, поэтому этот класс наследует классу List

        """
        def __init__(self, mapper, mapper_field_name, **kwargs):
            """
            @param mapper: Маппер основной таблицы
            @type mapper: Mapper

            @param mapper_field_name: Имя поля маппера
            @type mapper_field_name: str

            @param joined_collection: Тип привязанной коллекции
            @type joined_collection: TableModel
            """
            super().__init__(mapper, mapper_field_name, **kwargs)

            items_rel_field = self.items_collection_mapper.get_property_that_is_link_for(mapper)
            if self.items_collection_mapper.primary.name() == items_rel_field.get_name():
                raise TableMapperException("It is disallowed to create ReversedLink on primary key \"%s.%s\"" % (self.items_collection_mapper.table_name, items_rel_field.get_name()))

    class SqlEmbeddedLink(BaseReversedLink, SqlListWithoutRelationsTable):
        def clear_dependencies_from(self, main_records_ids: list):
            """
            Очищает зависимость присоединенных записей, от тех записей основной таблицы, которые были удалены и
            перечислены в main_records_ids
            @param main_records_ids: Список значений первичных ключей, удаленных записей основной таблицы
            @type main_records_ids: list

            """
            main_record_key = self.items_collection_mapper.get_property_that_is_link_for(self.mapper).get_name()
            self.items_collection_mapper.delete(
                {"%s.%s" % (main_record_key, self.mapper.primary.name()): ("in", main_records_ids)}
            )

    class SqlEmbeddedList(SqlListWithoutRelationsTable):

        def save_items(self, items: list, main_record_obj: RecordModel):
            """
            Сохраняет все привязанные к основному объекту объекты
            @param items: Список объектов
            @type items: list
            @param main_record_obj: Основной объект
            @type main_record_obj: RecordModel

            """
            main_record_key = self.items_collection_mapper.get_property_that_is_link_for(self.mapper).get_name()
            self.items_collection_mapper.delete(
                {"%s.%s" % (main_record_key, self.mapper.primary.name()): main_record_obj.get_actual_primary_value()}
            )
            for obj in filter(None, items):
                if self.items_collection_mapper.primary.autoincremented:
                    obj.unset_primary()
                item_data = obj.get_data_for_write_operation()
                item_data[main_record_key] = main_record_obj
                copy = obj.get_new_collection().get_new_item()
                copy.load_from_array(item_data)
                copy.save()

        def clear_dependencies_from(self, main_records_ids: list):
            """
            Очищает зависимость присоединенных записей, от тех записей основной таблицы, которые были удалены и
            перечислены в main_records_ids
            @param main_records_ids: Список значений первичных ключей, удаленных записей основной таблицы
            @type main_records_ids: list

            """
            main_record_key = self.items_collection_mapper.get_property_that_is_link_for(self.mapper).get_name()
            self.items_collection_mapper.delete(
                {"%s.%s" % (main_record_key, self.mapper.primary.name()): ("in", main_records_ids)}
            )

    ############################################## NoSql ###########################################################

    class NoSqlLink(BaseLink, NoSqlRelationField):
        pass

    class NoSqlList(BaseList, NoSqlRelationField):
        pass

    class NoSqlReversedList(NoSqlList, BaseForeignCollectionList):

        def __init__(self, mapper, mapper_field_name, **kwargs):
            """
            @param mapper: Маппер основной таблицы
            @type mapper: Mapper
            @param mapper_field_name: Имя поля маппера
            @type mapper_field_name: str
            @param joined_collection: Тип привязанной коллекции
            @type joined_collection: TableModel

            """
            super().__init__(mapper, mapper_field_name, **kwargs)
            self.db_field_name = mapper_field_name

    class NoSqlReversedLink(BaseReversedLink, NoSqlReversedList):
        ident = "ReversedLink"

        def __init__(self, mapper, mapper_field_name, **kwargs):
            """
            @param mapper: Маппер основной таблицы
            @type mapper: Mapper

            @param mapper_field_name: Имя поля маппера
            @type mapper_field_name: str

            @param joined_collection: Тип привязанной коллекции
            @type joined_collection: TableModel
            """
            super().__init__(mapper, mapper_field_name, **kwargs)
            items_rel_field = self.items_collection_mapper.get_property_that_is_link_for(mapper)
            if self.items_collection_mapper.primary.name() == items_rel_field.get_name():
                raise TableMapperException("It is disallowed to create ReversedLink on primary key \"%s.%s\"" % (self.items_collection_mapper.table_name, items_rel_field.get_name()))

    class NoSqlEmbeddedDocument(NoSqlRelationField):
        ident = "EmbeddedDocument"

        @abstractmethod
        def value_assertion(self, v):
            """ Проверяет корректность и соответствие этому типу поля значений
            @param v: Значение для проверки
            @return: Результат проверки

            """

    class NoSqlEmbeddedLink(NoSqlEmbeddedDocument):
        ident = "EmbeddedLink"

        def value_assertion(self, v):
            """ Проверяет корректность и соответствие этому типу поля значений
            @param v: Значение для проверки
            @return: Результат проверки

            """
            return isinstance(v, self.item_class) and isinstance(v, RecordModel) if v else True

        @staticmethod
        def is_primary_required():
            return False

    class NoSqlEmbeddedList(BaseList, NoSqlEmbeddedDocument):
            ident = "EmbeddedList"

            @staticmethod
            def is_primary_required():
                return False


class SqlMapper(metaclass=ABCMeta):
    """Класс создания Mapper'ов к таблицам БД """

    # Немного магии, чтобы экземпляры данного класса создавались только один раз...
    _instance = None
    _inited = False
    db = None
    dependencies = []
    support_joins = True

    def __new__(cls, *a, **kwa):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
        return cls._instance

    @classmethod
    def kill_instance(cls):
        """ Небольшой бэкдор для возможности уничтожение синглетного объекта """
        cls._instance = None
        cls._inited = False

    ###################################### Mapper Initialization ###################################################

    dublicate_record_exception = DublicateRecordException

    def __init__(self):
        if not self.__class__._inited:
            for dep in self.__class__.dependencies:
                dep()
            self.__class__._inited = True
            self.item_class = RecordModel
            self.item_collection_class = TableModel
            self.table_name = None                      # Имя основной таблицы
            self.db_fields = {}
            self.db_primary_key = ""
            self.primary = Primary(self)                # Объект, представляющий собой первичный ключ маппера
            self.boundaries = None
            self._properties = {}
            self._joined = OrderedDict()
            self._reversed_map = {}
            self.binded = False
            self.bind()                     # Запускаем процесс инициализации маппера
            self.binded = True

    @staticmethod
    def factory_method(item):
        """
        Фабричный метод, который может быть переопределен для настройки генерации маппером новых инстансов
        @param item: Базовый тип генерируемых маппером инстансов (тот, что установлен через set_new_item())
        @return:
        """
        return item

    def int(self, mapper_field_name, db_field_name):
        return FieldTypes.Int(self, mapper_field_name, db_field_name=db_field_name)

    def str(self, mapper_field_name, db_field_name):
        return FieldTypes.String(self, mapper_field_name, db_field_name=db_field_name)

    def repr(self, mapper_field_name, db_field_name):
        return FieldTypes.Repr(self, mapper_field_name, db_field_name=db_field_name)

    def bool(self, mapper_field_name, db_field_name):
        return FieldTypes.Bool(self, mapper_field_name, db_field_name=db_field_name)

    def float(self, mapper_field_name, db_field_name):
        return FieldTypes.Float(self, mapper_field_name, db_field_name=db_field_name)

    def date(self, mapper_field_name, db_field_name):
        return FieldTypes.Date(self, mapper_field_name, db_field_name=db_field_name)

    def time(self, mapper_field_name, db_field_name):
        return FieldTypes.Time(self, mapper_field_name, db_field_name=db_field_name)

    def datetime(self, mapper_field_name, db_field_name):
        return FieldTypes.DateTime(self, mapper_field_name, db_field_name=db_field_name)

    def link(self, mapper_field_name, db_field_name, collection):
        return FieldTypes.SqlLink(self, mapper_field_name, db_field_name=db_field_name, joined_collection=collection)

    def list(self, mapper_field_name, collection, rel_mapper):
        return FieldTypes.SqlListWithRelationsTable(
            self, mapper_field_name, joined_collection=collection, rel_mapper=rel_mapper
        )

    def reversed_link(self, mapper_field_name, collection):
        return FieldTypes.SqlReversedLink(self, mapper_field_name, joined_collection=collection)

    def reversed_list(self, mapper_field_name, collection):
        return FieldTypes.SqlListWithoutRelationsTable(self, mapper_field_name, joined_collection=collection)

    def embedded_link(self, mapper_field_name, collection):
        return FieldTypes.SqlEmbeddedLink(self, mapper_field_name, joined_collection=collection)

    def embedded_list(self, mapper_field_name, collection):
        return FieldTypes.SqlEmbeddedList(self, mapper_field_name, joined_collection=collection)

    def embedded_object(self, mapper_field_name, db_field_name, model):
        return FieldTypes.EmbeddedObject(self, mapper_field_name, db_field_name=db_field_name, model=model)

    def set_new_item(self, item_class):
        self.item_class = item_class

    def set_new_collection(self, collection_class):
        self.item_collection_class = collection_class

    def get_new_item(self):
        item = self.item_class()
        if not item.set_mapper(self):
            item.mapper = self
        return item

    def get_new_collection(self):
        collection = self.item_collection_class()
        if not collection.mapper:
            collection.mapper = self
        return collection

    @abstractmethod
    def bind(self):
        """
        Данный метод необходимо переопределить, для конфигурации маппера
        Внутри него необходимо вызвать self.attach() и self.set_map({...})

        """

    def set_map(self, mapper_fields: list):
        """
        Устанавливает список полей маппера
        @param mapper_fields: Список полей маппера
        @type mapper_fields: list

        """
        self._properties = {field.get_name(): field for field in mapper_fields}
        self.primary = Primary(self, name_in_db=self.db_primary_key)
        self._analyze_map()

    def set_field(self, field: FieldTypes.BaseField):
        """
        Определяет поля маппера. Если оно уже было определено - перезаписывает, иначе добавляет
        @param field: Новое или переопределенное поле
        @type field: FieldTypes.BaseField

        """
        self._properties[field.get_name()] = field
        self.primary = Primary(self, name_in_db=self.db_primary_key)
        self._analyze_map()

    def set_primary(self, field_name: str):
        """
        Переопределяет первичный ключ таблицы на уровне маппера
        @param field_name: Имя поля маппера, которое должно считаться первичным ключом
        @type field_name: str

        """
        self.primary = Primary(self, name_in_mapper=field_name)
        self.primary.defined_by_user = True
        self.db_primary_key = self.primary.db_name()
        self._analyze_map()

    def attach(self, table_name: str):
        """
        Линкует маппер к указанной таблице, с помощтю переданного коннекта к БД
        @param table_name: Имя таблицы БД
        @type table_name: str
        @deprecated
        """
        self.table_name = table_name
        self.db_fields, self.db_primary_key = self.db.get_table_fields(self.table_name)

    def set_collection_name(self, collection_name: str):
        """
        Линкует маппер к указанной таблице, с помощтю переданного коннекта к БД
        @param collection_name: Имя таблицы БД
        @type collection_name: str

        """
        self.attach(collection_name)

    def set_boundaries(self, boundaries: dict):
        """
        Устанавливает границы действия маппера
        @param boundaries: Словарь, определяющий границы
        """
        self.boundaries = boundaries

    def _analyze_map(self):
        """
        Анализирует имеющуюся информацию и
        а) создает обратную версию карты маппинга (Поля базы -> поля маппера)
        б) создает обратную версию карты маппинга (Имена внешних таблиц базы -> поля маппера)
        б) создает карту join'ов маппера и других таблиц

        """
        self._joined = OrderedDict()
        self._reversed_map = {}
        for mapperFieldName in self._properties:
            mapper_field = self._properties[mapperFieldName]
            self._reversed_map[mapper_field.get_db_name()] = mapper_field
            if isinstance(mapper_field, FieldTypes.RelationField):
                collection_mapper = mapper_field.get_items_collection_mapper()
                if isinstance(mapper_field, FieldTypes.SqlList):
                    if isinstance(mapper_field, FieldTypes.SqlListWithRelationsTable):
                        rel_mapper = mapper_field.get_relations_mapper()
                        first_mapper_key_in_rel_mapper = rel_mapper.get_property_that_is_link_for(self).get_db_name()
                        target_mapper_key_in_rel_mapper = rel_mapper.get_property_that_is_link_for(collection_mapper).get_db_name()
                        self.link_mappers(self, rel_mapper, self.db_primary_key, first_mapper_key_in_rel_mapper, rel_mapper.table_name, rel_mapper.table_name)
                        self.link_mappers(self, rel_mapper, self.db_primary_key, first_mapper_key_in_rel_mapper, mapper_field.get_name(), rel_mapper.table_name)
                        self.link_mappers(rel_mapper, collection_mapper, target_mapper_key_in_rel_mapper, collection_mapper.db_primary_key, mapper_field.get_name(), mapper_field.get_name())
                    else:
                        first_mapper_key_in_target_mapper = collection_mapper.get_property_that_is_link_for(self).get_db_name()
                        self.link_mappers(self, collection_mapper, self.db_primary_key, first_mapper_key_in_target_mapper, collection_mapper.table_name, collection_mapper.table_name)
                        self.link_mappers(self, collection_mapper, self.db_primary_key, first_mapper_key_in_target_mapper, mapper_field.get_name(), collection_mapper.table_name)
                        self.link_mappers(self, collection_mapper, self.db_primary_key, first_mapper_key_in_target_mapper, mapper_field.get_name(), mapper_field.get_name())
                else:
                    self.link_mappers(self, collection_mapper, mapper_field.get_db_name(), collection_mapper.db_primary_key, mapper_field.get_name(), mapper_field.get_name())

    def link_mappers(self, first_mapper, second_mapper, first_key, second_key, target, alias):
        if target not in self._joined:
            self._joined[target] = OrderedDict()
        self._joined[target][(first_mapper.table_name, first_key)] = (
            second_mapper.table_name, second_key, alias
        )

    def get_properties(self) -> list:
        """
        Возвращает список публично доступных свойств маппера
        @return: Список полей маппера
        @rtype : list

        """
        return list(self._properties.keys())

    def get_property(self, field_name: str) -> FieldTypes.BaseField:
        """
        Возвращаеет поле маппера по его имени
        @param field_name: Имя поля маппера, объект которого требуется получить
        @type field_name: str
        @return: Объект поля маппера
        @rtype : FieldTypes.BaseField

        """
        return self._properties.get(field_name)

    def get_property_by_db_name(self, db_name: str) -> FieldTypes.BaseField:
        """
        Возвращает поле маппера по имени соответствующего ему поля в базе данных
        @param db_name: Имя поля в базе данных, с которым ассоциировано требуемое поле маппера
        @type db_name: str
        @return: Объект поля маппера
        @rtype : FieldTypes.BaseField

        """
        return self._reversed_map.get(db_name)

    def get_property_that_is_link_for(self, foreign_mapper) -> FieldTypes.BaseField:
        """
        Возвращает имя поля маппера, соответствующее внешнему мапперу
        @param foreign_mapper: Внешний маппер, для которого требуется вернуть ассоциированное поле маппера
        @return: Объект поля маппера, ассоциированного с указанным внешним маппером
        @rtype : FieldTypes.BaseField

        """
        for prop in self.get_properties():
            prop = self.get_property(prop)
            if self.is_rel(prop):
                # noinspection PyUnresolvedReferences
                if prop.get_items_collection_mapper() == foreign_mapper:
                    return prop

    def get_joined_tables(self, conditions: dict, fields: list=None, order=None):
        """
        Возвращает часть словаря _joined, оставив только те таблицы, которые используются в conditions и fields
        @param conditions: Условия выборки
        @type conditions: dict
        @param fields: Поля для выборки
        @type fields: list
        @return: Словарь с данными ог присоединенных таблицах
        @rtype : dict

        """
        if order:
            if type(order) is tuple:
                order = [order]
            order_fields = [f[0] for f in order]
        else:
            order_fields = []
        fields, conditions = fields if fields else [], conditions if conditions else {}
        foreign_tables = list(set(filter(
            None,
            [field.split(".")[0] if field.find(".") > -1 else None for field in
             (fields + list(conditions.keys()) + order_fields)])))
        return {joined_table_name: self._joined[joined_table_name] for joined_table_name in foreign_tables}

    def get_db_type(self, field_name: str) -> str:
        """
        Возвращает тип поля таблицы в БД по его имени
        @param field_name: Имя поля таблицы базы данных
        @type field_name: str
        @return: Тип поля в базе данных
        @rtype : str
        @raise TableMapperException: Если такого поля в таблице нет

        """
        field = self.db_fields.get(field_name)
        if field is None:
            raise TableMapperException("there is no field named '%s' in table '%s'" % (field_name, self.table_name))
        return field.db_type

    ######## Следующие методы необходимы чтобы инкапсулировать работу с классами полей внутри маппера ################

    def prop_wrap(self, mf: str or FieldTypes.BaseField) -> str:
        """
        Обрабатывает переданное значение так, что если передано имя поля маппера, то превращает его в объект поля,
        а если передан объект поля маппера - оставляет его как есть
        @param mf: Имя поля маппера или объект поля маппера
        @type mf: str or FieldTypes.BaseField
        @return: Объект поля маппера
        @rtype : FieldTypes.BaseField

        """
        return mf if isinstance(mf, FieldTypes.BaseField) else self._properties.get(mf)

    def is_rel(self, mf: FieldTypes.BaseField) -> bool:
        mf = self.prop_wrap(mf)
        return isinstance(mf, FieldTypes.RelationField)

    def is_link(self, mf: FieldTypes.BaseField) -> bool:
        return isinstance(self.prop_wrap(mf), FieldTypes.BaseLink)

    def is_list(self, mf: FieldTypes.BaseField) -> bool:
        return isinstance(self.prop_wrap(mf), FieldTypes.BaseList)

    def is_list_with_dependencies(self, mf: FieldTypes.BaseField) -> bool:
        return isinstance(self.prop_wrap(mf), FieldTypes.BaseForeignCollectionList)

    def is_reversed_list(self, mf: FieldTypes.BaseField) -> bool:
        return isinstance(self.prop_wrap(mf), FieldTypes.BaseForeignCollectionList)

    def is_reversed_link(self, mf: FieldTypes.BaseField) -> bool:
        return isinstance(self.prop_wrap(mf), FieldTypes.BaseReversedLink)

    def is_real_embedded(self, mf: FieldTypes.BaseField) -> bool:
        return isinstance(self.prop_wrap(mf), FieldTypes.NoSqlEmbeddedDocument)

    def is_embedded_object(self, mf: FieldTypes.BaseField) -> bool:
        return isinstance(self.prop_wrap(mf), FieldTypes.EmbeddedObject)

    @staticmethod
    def is_base_value(value) -> bool:
        return isinstance(value, FieldValues.BaseValue)

    @staticmethod
    def get_base_none():
        return FieldValues.NoneValue()

    ############################################ CRUD ###############################################################

    def generate_rows(self, fields: list=None, conditions: dict=None, params: dict=None, cache=None):
        """
        Делаем выборку данных, осуществляя функции маппинга строк (названия полей + формат значений)
        @param fields: Список полей для получения
        @type fields: list
        @param conditions: Условия выборки
        @type conditions: dict
        @param params: Параметры выборки
        @type params: dict
        @param cache: Используемый кэш

        """
        fields = self.translate_and_convert(fields) \
            if fields not in [[], None] else list(self._reversed_map.keys())

        conditions = self.translate_and_convert(conditions)
        params = self.convert_params(params) if params else {}
        joined_tables = self.get_joined_tables(conditions, fields, params.get("order"))

        for row in self.db.select_query(self.table_name, fields, conditions, params, joined_tables, "get_rows"):
            result = {fields[it]: row[it] for it in range(len(fields))}
            yield self.translate_and_convert(result, "database2mapper", cache)

    def get_value(self, field_name: str, conditions: dict=None):
        """
        Возвращает значение поля записи, соответствующей условиям выборки
        @param field_name: Имя поля
        @type field_name: str
        @param conditions: Условия выборки записи
        @type conditions: dict
        @return: Значение искомого поля
        """
        result = list(self.get_rows([field_name], conditions))
        return result[0].get(field_name) if len(result) > 0 else None

    def get_column(self, column_name: str, conditions: dict=None, params: dict=None) -> list:
        """
        Возвращает список значений одной из колонок таблицы в соответствии с условиями и параметрами выборки
        @param column_name: Имя колонки таблицы
        @type column_name: str
        @param conditions: Условия выборки записи
        @type conditions: dict
        @param params: Параметры выборки
        @type params: dict
        @return: Список значений для указанной колонки, взятых из строк, удовлетворяющих условиям выборки
        @rtype : dict

        """
        for row in self.generate_rows([column_name], conditions, params):
            if None != row.get(column_name):
                yield row.get(column_name)

    def get_row(self, fields: list=None, conditions: dict=None) -> dict:
        """
        Возвращает словарь с данными для одной записи, удовлетворяющей условиям выборки
        @param fields: Список полей
        @type fields: list
        @param conditions: Условия выборки записи
        @type conditions: dict
        @return: Словарь с данными выбранной записи
        @rtype : dict

        """
        result = list(self.get_rows(fields, conditions))
        return result[0] if len(result) == 1 else None

    def get_rows(self, fields: list=None, conditions: dict=None, params: dict=None, cache=None) -> list:
        """
        Возвращает список записей (в виде словарей), соответствующих условиям выборки
        @param fields: Список полей для получения
        @type fields: list
        @param conditions: Условия выборки записи
        @type conditions: dict
        @param params: Параметры выборки
        @type params: dict
        @param cache: Используемый кэш
        @return: Список словарей данных найденных записей
        @rtype: list

        """
        for row in self.generate_rows(fields, conditions, params, cache):
            yield row

    def count(self, conditions: dict=None) -> int:
        """
        Выполняем подсчет записей в коллекции по условию
        @param conditions: Условия подсчета строк
        @type conditions: dict
        @return: Количество строк, соответствующих условиям подсчета
        @rtype : int
        """
        conditions = self.translate_and_convert(conditions)
        return self.db.count_query(self.table_name, conditions, self.get_joined_tables(conditions))

    def insert(self, data: list or dict):
        """
        Выполняет вставку новой записи в таблицу
        Для вставки одной записи параметр data должен быть словарем, для вставки нескольких - списком словарей
        @param data: Данные для вставки
        @type data: list or dict
        @return: Значение первичного ключа для добавленной записи
        
        """
        if type(data) is list:
            return [self.insert(it) for it in data]

        if not isinstance(data, dict):
            raise TableMapperException("Insert failed: unknown item format")
        elif data == {}:
            raise TableModelException("Can't insert an empty record")

        try:
            last_record = self.db.insert_query(self.table_name, self.translate_and_convert(data), self.primary)
        except DublicateRecordException as err:
            raise self.__class__.dublicate_record_exception(err)
        return self.primary.grab_value_from(
            last_record if self.primary.defined_by_user is False and last_record != 0 else data
        )

    def update(self, data: dict, conditions: dict=None, new_item=None):
        """
        Выполняет обновление существующих в таблице записей
        @param data: Новые данные
        @type data: dict
        @param conditions: Условия выборки записей для применения обновлений
        @type conditions: dict
        @param new_item: Экземпляр класса модели, соответствующей основной добавляемой записи
        @return: Значение первичного ключа для обновленной записи/записей
        
        """
        flat_data, lists_objects = self.split_data_by_relation_type(data)

        # Отсекаем из массива изменений, то,
        # что в неизменном виде присутствует в массиве условий (то есть не будет изменено)
        # Это важно особенно, так как в flat_data не должны попадать те значения первичных ключей, которые не изменялись
        # Так как они могут быть что-то вроде IDENTITY полей и не подлежат изменениям даже на теже самые значения
        if conditions:
            flat_data = {key: flat_data[key] for key in flat_data if conditions.get(key, "&bzx") != flat_data[key]}

        # Сохраняем записи в основной таблице
        if flat_data != {}:
            converted_conditions = self.translate_and_convert(conditions)
            try:
                self.db.update_query(
                    self.table_name, self.translate_and_convert(flat_data), converted_conditions,
                    self.get_joined_tables(converted_conditions), self.primary
                )
            except DublicateRecordException as err:
                raise self.__class__.dublicate_record_exception(err)
        # Получаем id измененных записей и пересохраняем привязанные к ним объекты (если требуется)
        if lists_objects != {}:
            if self.primary.exists():           # Если, конечно, первичный ключ определен
                if self.primary.compound:       # Если он составной
                    changed_records_ids = [
                        self.primary.grab_value_from(chid) for chid in self.get_rows([self.primary.name()], conditions)
                    ]
                else:                           # Если он обычный
                    changed_records_ids = list(self.get_column(self.primary.name(), conditions))
                if new_item:
                    for main_record_id in changed_records_ids:
                        self.link_all_list_objects(lists_objects, new_item().load_by_primary(main_record_id))
                return changed_records_ids
            else:
                raise TableModelException("Updates can affect only main mapper's fields since there is no primary key")

    def delete(self, conditions: dict=None):
        """
        Подсчитывает количество строк в таблице, в соответствии с условиями (если они переданы)
        @param conditions: Параметры выборки для удаления строки из таблицы
        @type conditions: dict
        @return:

        """
        if self.primary.exists() and self.primary.compound is False:           # Если, конечно, первичный ключ определен
            changed_records_ids = list(
                [
                    i.get_value() if isinstance(i, EmbeddedObject) else
                    i.get_actual_primary_value() if isinstance(i, RecordModel) else
                    i
                    for i in self.get_column(self.primary.name(), conditions)
                ]
            )
            if len(changed_records_ids) > 0:
                self.unlink_objects(changed_records_ids)
                self.db.delete_query(
                    self.table_name,
                    self.translate_and_convert({self.primary.name(): ("in", changed_records_ids)}), {}
                )
                return changed_records_ids
        else:
            conditions = self.translate_and_convert(conditions)
            self.db.delete_query(self.table_name, conditions, self.get_joined_tables(conditions))

    def split_data_by_relation_type(self, data: dict) -> (dict, dict):
        """
        Разбивает данные на две части, относящиеся к основной таблице и привязанные связью многие-ко-многим
        @param data: Данные для разбивки
        @type data: dict
        @raise TableMapperException:   Если указано поле, которого у маппера реально нет
        @return: Два словаря: один с данными основной таблицы, второй - с привязанными к основному объекту моделями
        @rtype : (dict, dict)

        """
        lists, flat = {}, {}
        for mapperFieldName in data:
            mapper_field = self.get_property(mapperFieldName)
            if self.is_list(mapper_field):
                lists[mapper_field] = data[mapperFieldName]
            else:
                flat[mapperFieldName] = data[mapperFieldName]
        return flat, lists

    @staticmethod
    def link_all_list_objects(data: dict, main_record_obj: RecordModel):
        """
        Сохраняет связь основной записи таблицы и других объектов, если они связаны как многие-ко-многим
        @param data: Привязанные объекты
        @type data: dict
        @param main_record_obj: Объект основной записи
        @type main_record_obj: RecordModel

        """
        for mapper_field in data:
            mapper_field.save_items(data[mapper_field], main_record_obj)

    def unlink_objects(self, changed_records_ids):
        for mapper_field_name in self.get_properties():
            mapper_field = self.get_property(mapper_field_name)
            if self.is_list_with_dependencies(mapper_field):
                # noinspection PyUnresolvedReferences
                mapper_field.clear_dependencies_from(changed_records_ids)

    ##################################################################################################################
    def translate_and_convert(self, value, direction: str="mapper2database", cache=None):
        """
        Осуществляет непосредственное конвертирование данных из формата маппера в формат бд и наоборот
        @param value: Объект для конвертации (может быть простым значением, списком, словарем)
        @param direction: Направление конвертации
        @type direction: str
        @param cache: Используемый в запросе объект-кэш
        @return: Конвертированный объект
        """
        if type(value) is list:
            return [self.translate_and_convert(newvalue, direction, cache) for newvalue in value]
        elif type(value) is dict:
            converted = {}
            for field in value:
                if field in ["or", "and"]:
                    converted[field] = self.translate_and_convert(value[field], direction, cache)
                else:
                    translted_field = self.translate_and_convert(field, direction, cache)
                    if type(value[field]) is tuple:
                        converted[translted_field] = value[field]
                    else:
                        mapper_field = self.get_mapper_field(field, direction)
                        converted[translted_field] = mapper_field.convert(value[field], direction, cache)
            return converted
        elif type(value) is str:
            return self.translate(value, direction)
        else:
            return value

    def translate(self, name: str, direction: str) -> str:
        """
        Конвертирует обращение к свойству маппера к обращению к полю таблицы БД
        или наоборот в зависимости от direction
        @param name: Имя поля для перевода
        @type name: str
        @param direction: Направление конвертации
        @type direction: str
        @rtype : str

        """
        return self.get_mapper_field(name, direction, first=True).translate(name, direction)

    def get_mapper_field(self, field_name: str, direction: str, first: bool=False) -> FieldTypes.BaseField:
        """
        Возвращает объект поля маппера по переданному текстовому обращению
        @param field_name: Обращение к  какому-либо полю (либо в терминах маппера, либо в терминах бд)
        @type field_name: str
        @param direction: Направление конвертации
        @type direction: str
        @param first: Нужно ли вернуть первый найденный объект или следовать цепочке (Например, user.account.name)
        @type first: bool
        @return: Экземпляр поля маппера
        @rtype : FieldTypes.BaseField

        """
        if field_name.endswith("]"):
            mapper_field_name = re.search("\[(.+?)\]", field_name).group(1)
            return self.get_property(mapper_field_name)
        if field_name.find(".") > -1:
            mapper_field_name, mapper_field_property = field_name.split(".")
            if direction == "database2mapper":
                foreign_table, foreign_table_field_name = mapper_field_name, mapper_field_property
                mapper_field_name = self.get_property(foreign_table).get_name()
                first_mapper_field = self.get_property(mapper_field_name)
                # noinspection PyUnresolvedReferences
                linked_mapper = first_mapper_field.get_items_collection_mapper()
                mapper_field_property = linked_mapper.translate(foreign_table_field_name, direction)
            else:
                first_mapper_field = self.get_property(mapper_field_name)
                # noinspection PyUnresolvedReferences
                linked_mapper = first_mapper_field.get_items_collection_mapper()
            return first_mapper_field if first else linked_mapper.get_property(mapper_field_property)
        res = self._reversed_map.get(field_name)
        return res if res else self.get_property(field_name)

    def convert_params(self, params: dict) -> dict:
        """
        Маппинг имен полей, используемых в параметрах выборки
        @param params: Параметры выборки
        @type params: dict
        @return: Результат конвертации параметров выборки
        @rtype : dict

        """
        if params is not None:
            if type(params.get("order")) is tuple:
                params["order"] = (self.translate_and_convert(params["order"][0]), params["order"][1])
            elif type(params.get("order")) is list:
                params["order"] = [(self.translate_and_convert(ordOpt[0]), ordOpt[1]) for ordOpt in params["order"]]
        return params

    @staticmethod
    def get_type_for_primary():
        """ Возвращает класс, используемый для работы с первичными ключами маппера """
        return Primary


class NoSqlMapper(SqlMapper, metaclass=ABCMeta):
    """ Класс для создания мапперов к NoSql коллекциям """
    support_joins = False

    def object_id(self, mapper_field_name, db_field_name):
        return FieldTypes.NoSqlObjectID(self, mapper_field_name, db_field_name=db_field_name)

    def int(self, mapper_field_name, db_field_name, db_field_type=None):
        return FieldTypes.NoSqlInt(self, mapper_field_name, db_field_name=db_field_name,
                                   db_field_type=db_field_type if db_field_type else FieldTypes.NoSqlInt)

    def str(self, mapper_field_name, db_field_name, db_field_type=None):
        return FieldTypes.NoSqlString(self, mapper_field_name, db_field_name=db_field_name,
                                      db_field_type=db_field_type if db_field_type else FieldTypes.NoSqlString)

    def repr(self, mapper_field_name, db_field_name, db_field_type=None):
        return FieldTypes.Repr(self, mapper_field_name, db_field_name=db_field_name,
                               db_field_type=db_field_type if db_field_type else FieldTypes.NoSqlString)

    def bool(self, mapper_field_name, db_field_name, db_field_type=None):
        return FieldTypes.NoSqlBool(self, mapper_field_name, db_field_name=db_field_name,
                                    db_field_type=db_field_type if db_field_type else FieldTypes.NoSqlBool)

    def float(self, mapper_field_name, db_field_name, db_field_type=None):
        return FieldTypes.NoSqlFloat(self, mapper_field_name, db_field_name=db_field_name,
                                     db_field_type=db_field_type if db_field_type else FieldTypes.NoSqlFloat)

    def date(self, mapper_field_name, db_field_name, db_field_type=None):
        return FieldTypes.NoSqlDate(self, mapper_field_name, db_field_name=db_field_name,
                                    db_field_type=db_field_type if db_field_type else FieldTypes.NoSqlDate)

    def time(self, mapper_field_name, db_field_name, db_field_type=None):
        return FieldTypes.NoSqlTime(self, mapper_field_name, db_field_name=db_field_name,
                                    db_field_type=db_field_type if db_field_type else FieldTypes.NoSqlTime)

    def datetime(self, mapper_field_name, db_field_name, db_field_type=None):
        return FieldTypes.NoSqlDateTime(self, mapper_field_name, db_field_name=db_field_name,
                                        db_field_type=db_field_type if db_field_type else FieldTypes.NoSqlDateTime)

    def link(self, mapper_field_name, db_field_name, collection, db_field_type=None):
        return FieldTypes.NoSqlLink(
            self, mapper_field_name,
            db_field_type=db_field_type if db_field_type else FieldTypes.NoSqlObjectID,
            db_field_name=db_field_name,
            joined_collection=collection
        )

    def list(self, mapper_field_name, db_field_name, collection, db_field_type=None):
        return FieldTypes.NoSqlList(
            self, mapper_field_name,
            db_field_type=db_field_type if db_field_type else FieldTypes.NoSqlObjectID,
            db_field_name=db_field_name,
            joined_collection=collection
        )

    def reversed_link(self, mapper_field_name, collection, db_field_type=None):
        return FieldTypes.NoSqlReversedLink(
            self, mapper_field_name,
            db_field_type=db_field_type if db_field_type else FieldTypes.NoSqlObjectID,
            joined_collection=collection
        )

    def reversed_list(self, mapper_field_name, collection, db_field_type=None):
        return FieldTypes.NoSqlReversedList(
            self, mapper_field_name,
            db_field_type=db_field_type if db_field_type else FieldTypes.NoSqlObjectID,
            joined_collection=collection
        )

    # noinspection PyMethodOverriding
    def embedded_link(self, mapper_field_name, db_field_name, collection):
        return FieldTypes.NoSqlEmbeddedLink(
            self, mapper_field_name,
            db_field_name=db_field_name, db_field_type=FieldTypes.NoSqlEmbeddedDocument,
            joined_collection=collection
        )

    # noinspection PyMethodOverriding
    def embedded_list(self, mapper_field_name, db_field_name, collection):
        return FieldTypes.NoSqlEmbeddedList(
            self, mapper_field_name,
            db_field_name=db_field_name, db_field_type=FieldTypes.NoSqlEmbeddedDocument,
            joined_collection=collection
        )

    def embedded_object(self, mapper_field_name, db_field_name, model):
        return FieldTypes.NoSqlEmbeddedObject(self, mapper_field_name, db_field_name=db_field_name, model=model)

    @abstractmethod
    def bind(self):
        """ Переопределить в суб-класса  """

    def get_joined_tables(self, conditions: dict, fields: list=None, order=None):
        """ Переопределяем базовый метод, join'ы не поддерживаются """
        return {}

    def split_data_by_relation_type(self, data: dict) -> (dict, dict):
        """ Переопределяем базовый метод так, чтобы он не отделял значения типа list от общей массы данных """
        lists, flat = {}, {}
        for mapperFieldName in data:
            mapper_field = self.get_property(mapperFieldName)
            if self.is_list_with_dependencies(mapper_field):
                lists[mapper_field] = data[mapperFieldName]
            else:
                flat[mapperFieldName] = data[mapperFieldName]
        return flat, lists

    def generate_rows(self, fields: list=None, conditions: dict=None, params: dict=None, cache=None):
        """
        Делаем выборку данных, осуществляя функции маппинга строк (названия полей + формат значений)
        @param fields: Список полей для получения
        @type fields: list
        @param conditions: Условия выборки
        @type conditions: dict
        @param params: Параметры выборки
        @type params: dict
        @param cache: Используемый кэш

        """
        # Анализируем список запрошенных полей
        main_collection_fields = [field.split(".")[0] for field in fields]
        reversed_collections = [key for key in self.get_properties() if self.is_reversed_list(self.get_property(key))]
        embedded_collections = [key for key in main_collection_fields if self.is_real_embedded(self.get_property(key))]
        main_collection_fields = self.translate_and_convert(main_collection_fields)
        if len(main_collection_fields) > 0:
            main_collection_fields.append("_id")
            reversed_collections = list(set(reversed_collections) & set(main_collection_fields))

        # Разбиваем условия выборки на группы в соответствии с коллекцией:
        collection_conditions = {"self": self.translate_and_convert(conditions)}
        if conditions:
            for key in conditions:
                if key.find(".") > -1 and self.is_rel(self.get_property(key.split(".")[0])):
                    collection_conditions[key.split(".")[0]] = {key.split(".")[1]: conditions[key]}
        # Конвертируем параметры выборки
        params = self.convert_params(params)

        # Сохраняем данные основных записей, их первичные ключи и ключи внешних моделей, прилинкованнык к записям:
        rows = []
        rows_primaries = []
        foreign_models_primaries = defaultdict(list)
        foreign_models_by_row = {field: defaultdict(list) for field in main_collection_fields}
        for row in self.db.select_query(self.table_name, main_collection_fields, collection_conditions["self"], params):
            rows.append(row)
            rows_primaries.append(row.get("_id"))
            for key in row:
                key_in_mapper = self.translate_and_convert(key, "database2mapper")
                mf = self.get_property(key_in_mapper)
                if mf and self.is_rel(mf) and not self.is_real_embedded(mf):
                    if type(row[key]) is list:
                        for obid in row[key]:
                            if key in foreign_models_by_row:
                                foreign_models_by_row[key][row["_id"]].append(obid)
                            foreign_models_primaries[key_in_mapper].append(obid)
                    else:
                        if key in foreign_models_by_row:
                            foreign_models_by_row[key][row["_id"]].append(row[key])
                        foreign_models_primaries[key_in_mapper].append(row[key])
        foreign_models_primaries = dict(foreign_models_primaries)
        foreign_models_by_row = dict(foreign_models_by_row)

        # Сохраняем первичные ключи реверсных моделей:
        reversed_models_primaries = defaultdict(list)
        reversed_models_by_row = {prop: defaultdict(list) for prop in reversed_collections}
        for prop in reversed_collections:
            # noinspection PyUnresolvedReferences
            linked_mapper = self.get_property(prop).get_items_collection_mapper()
            main_record_key = linked_mapper.get_property_that_is_link_for(self)
            rev_collection_condtions = {main_record_key.get_db_name(): {"$in": rows_primaries}}
            if collection_conditions.get(prop):
                rev_collection_condtions.update(linked_mapper.translate_and_convert(collection_conditions.get(prop)))
            requested_fields = ["_id", main_record_key.get_db_name()]
            data = linked_mapper.db.select_query(linked_mapper.table_name, requested_fields, rev_collection_condtions)
            for reversed_model in data:
                reversed_models_by_row[prop][reversed_model[main_record_key.get_db_name()]].append(
                    reversed_model.get("_id")
                )
            for list_objid in reversed_models_by_row[prop].values():
                for objid in list_objid:
                    reversed_models_primaries[prop].append(objid)
        reversed_models_primaries = dict(reversed_models_primaries)
        reversed_models_by_row = dict(reversed_models_by_row)

        # Объединяем реверсные модели и обычные внешние модели, так как далее логика сходится
        foreign_models_primaries.update(reversed_models_primaries)
        foreign_models_by_row.update(reversed_models_by_row)

        if fields and len(fields) > 0:
            # Если свойства запрошены у внешних моделей, связанных с основными записями:
            if foreign_models_primaries != {}:
                for key in foreign_models_primaries:
                    mf = self.get_property(key)
                    # noinspection PyUnresolvedReferences
                    linked_mapper = mf.get_items_collection_mapper()
                    key_in_db = mf.get_db_name()
                    mapper_type_models = foreign_models_by_row.get(key_in_db)

                    # Создаем словарь с условиями выборки моделей
                    basic_conditions = {"_id": {"$in": foreign_models_primaries[key]}}
                    if collection_conditions["self"] and collection_conditions["self"].get(key_in_db):
                        basic_conditions.update({"_id": collection_conditions["self"].get(key_in_db)})

                    # Создаем список для заполнения полей выбираемых моделей
                    requested_fields = [field.split(".")[1] for field in fields if field.split(".")[0] == key]
                    db_fields = linked_mapper.translate_and_convert(requested_fields)
                    db_fields.append("_id")

                    # Заполняем модели
                    foreign_models = {}
                    for row in linked_mapper.db.select_query(linked_mapper.table_name, db_fields, basic_conditions):
                        foreign_models[row["_id"]] = row

                    for main_record in rows:
                        main_record_for_yield = self.translate_and_convert(main_record, "database2mapper", cache)
                        main_record_for_yield = {
                            key: main_record_for_yield[key] for key in main_record_for_yield if key in fields
                        }
                        row_models = mapper_type_models.get(main_record["_id"])
                        if row_models:
                            models_in_this_row = [foreign_models.get(rm) for rm in row_models]
                            for item in models_in_this_row:
                                if item:
                                    item = {
                                        field: item[linked_mapper.translate(field.split(".")[1], "mapper2database")]
                                        for field in fields if field.find(".") > -1
                                    }
                                    item.update(main_record_for_yield)
                                    yield item
                                else:
                                    if main_record_for_yield != {}:
                                        yield main_record_for_yield
                        else:
                            yield main_record_for_yield
                return
            # Если свойства запрошены у вложенных документов основных записей
            elif len(embedded_collections) > 0:
                for row in rows:
                    row = self.translate_and_convert(row, "database2mapper", cache)
                    for subcollection in row:
                        if self.is_real_embedded(self.get_property(subcollection)):
                            if type(row[subcollection]) is not list:
                                row[subcollection] = [row[subcollection]]
                            for item in row[subcollection]:
                                if self.document_match(item, subcollection, collection_conditions):
                                    item = item.__dict__
                                    item = {
                                        field: item[field.split(".")[1]] for field in fields if field.find(".") > -1
                                    }
                                    main_record = {field: row[field] for field in fields if row.get(field)}
                                    item.update(main_record)
                                    yield item
                return
        # В остальных случаях (либо вообще без свойство - целые модели, либо свойства основных записей)
        for row in rows:
            for reversed_model_key in reversed_models_by_row:
                reversed_model_trait = {reversed_model_key: reversed_models_by_row[reversed_model_key].get(row["_id"])}
                row.update(reversed_model_trait)
            if not self.get_property_by_db_name("_id") or (len(fields) > 0 and self.primary.name() not in fields):
                del row["_id"]
            yield self.translate_and_convert(row, "database2mapper", cache)

    @staticmethod
    def document_match(model, property_name, conditions):
        """
        Сравнивает переданную модель с переданными условиями
        @param model: Модель для проверки соответствия условиям
        @param property_name: Имя embedded коллекции, частью которой является проверямая модель
        @param conditions: Условия выборки
        @return:
        """
        conditions = conditions.get(property_name)
        if conditions:
            for key in conditions:
                if type(conditions[key]) is not tuple:
                    conditions[key] = ("e", conditions[key])
                operator, value = conditions[key]
                option = model.get_data().get(key)
                if not option:
                    continue
                elif operator == "in" and option not in value:
                    return False
                elif operator == "nin" and option in value:
                    return False
                elif operator == "e" and option != value:
                    return False
                elif operator == "ne" and option == value:
                    return False
                elif operator == "gt" and option <= value:
                    return False
                elif operator == "gte" and option < value:
                    return False
                elif operator == "lt" and option >= value:
                    return False
                elif operator == "lte" and option > value:
                    return False
                elif operator == "match" and option.find(value) == -1:
                    return False
        return True

    def convert_conditions_to_one_collection(self, conditions):
        """
        Преобразует слвоарь с уловиями выборки от формата обращения к смежным таблицам,
        к формату обращения только к одной основной таблице за счет замены части условий,
        относящихся к другим таблицам на условия выборки по первичными ключам этих таблиц.
        @param conditions: Исходные условия
        @return: Сконвертированный результат
        """
        new_conditions = {}
        for key in conditions:
            if key.find(".") > -1:
                mapper_field_name, other_mapper_property_name = key.split(".")
                mf = self.get_property(mapper_field_name)
                if self.is_real_embedded(mf):
                    new_conditions["%s.%s" % (mf.get_name(), other_mapper_property_name)] = conditions[key]
                elif self.is_rel(mf):
                    # noinspection PyUnresolvedReferences
                    fmapper = mf.get_items_collection_mapper()
                    if self.is_list_with_dependencies(mf):
                        sub_conditions = fmapper.db.select_query(
                            fmapper.table_name, [fmapper.get_property_that_is_link_for(self).get_db_name()],
                            self.to_mongo_conditions_format(
                                {fmapper.translate(other_mapper_property_name, "mapper2database"): conditions[key]}
                            )
                        )
                        new_conditions["_id"] = (
                            "in",
                            [el[fmapper.get_property_that_is_link_for(self).get_db_name()] for el in sub_conditions]
                        )
                    else:
                        sub_conditions = fmapper.db.select_query(
                            fmapper.table_name, [fmapper.primary.db_name()],
                            self.to_mongo_conditions_format(
                                {fmapper.translate(other_mapper_property_name, "mapper2database"): conditions[key]}
                            )
                        )
                        new_conditions[mf.get_db_name()] = (
                            "in",
                            [el[fmapper.primary.db_name()] for el in sub_conditions]
                        )
            else:
                new_conditions[key] = conditions[key]
        return new_conditions

    def translate_and_convert(self, value, direction: str="mapper2database", cache=None):
        """
        Осуществляет непосредственное конвертирование данных из формата маппера в формат бд и наоборот
        @param value: Объект для конвертации (может быть простым значением, списком, словарем)
        @param direction: Направление конвертации
        @type direction: str
        @param cache: Используемый в запросе объект-кэш
        @return: Конвертированный объект
        """
        if direction == "database2mapper" and value == "_id" and self.get_property_by_db_name(value) is None:
            return value
        if type(value) is dict:
            value = self.convert_conditions_to_one_collection(value)
            value = super().translate_and_convert(value, direction, cache)
            value = self.to_mongo_conditions_format(value)
        else:
            value = super().translate_and_convert(value, direction, cache)
        return value

    @staticmethod
    def to_mongo_conditions_format(conditions):
        """
        Преобразует словарь с сопоставлениями к формату, используемому в mongodb
        @param conditions: Словарь с условиями
        @return: Преобразованный словарь
        """
        for key in conditions:
            # Обрабатываем случае конъюнкции и дизъюнкции
            if key in ["and", "or"]:
                conditions["$%s" % key] = [NoSqlMapper.to_mongo_conditions_format(sub) for sub in conditions[key]]
                del conditions[key]
                continue

            # Обрабатываем случай проверки наличия значения в поле
            if type(conditions[key]) is tuple:
                if conditions[key][0] == "exists":
                    conditions[key] = ("ne" if conditions[key] else "e", None)

            # Обрабатываем случай проверки вхождения подстроки в строку
            if type(conditions[key]) is tuple:
                if conditions[key][0] == "match":
                    conditions[key] = ("regex", "^%s$" % conditions[key][1].replace("*", ".*"))

            # Конвертируем все остальные операторы сравнения
            if type(conditions[key]) is tuple:
                conditions[key] = {"$%s" % conditions[key][0]: conditions[key][1]}
        return conditions


class FieldValues(object):
    """ Классы для эмуляции значений свойств моделей """

    class BaseValue(object):
        """ Базовый класс для всех возможных типов значений свойств моделей """
        def __init__(self):
            self.changed = False

    # noinspection PyDocstring
    class ListValue(BaseValue, list):
        """ Специальный класс для замены обычных списков - возвращается при создании списков объектов моделей """

        def __init__(self, iterable=None):
            if iterable is None:
                iterable = []
            list.__init__(self, iterable)
            super().__init__()

        def insert(self, i, v):
            super().insert(i, v)
            self.changed = True

        def append(self, p_object):
            super().append(p_object)
            self.changed = True

        def __delitem__(self, key):
            super().__delitem__(key)
            self.changed = True

        def __setitem__(self, key, value):
            super().__setitem__(key, value)
            self.changed = True

        def __add__(self, other):
            super().__add__(other)
            self.changed = True

        def __iadd__(self, other):
            super().__iadd__(other)
            self.changed = True

        def __imul__(self, other):
            super().__imul__(other)
            self.changed = True

    class NoneValue(BaseValue):
        """ Специальный класс для замены обычных пустых значений """
        def __init__(self):
            super().__init__()
            self.changed = False

        def __eq__(self, other):
            return other is None or isinstance(other, FieldValues.NoneValue)

        def __ne__(self, other):
            return other is not None and isinstance(other, FieldValues.NoneValue) is False

        def __bool__(self):
            return False

        def __len__(self):
            return 0
        
        def __repr__(self):
            return "None"


class FNone(FieldValues.NoneValue):
    """ Алиас для FieldValues.NoneValue для краткой записи внутри FieldTypesConverter """


class FieldTypesConverter(object):
    """ Конвертер значений разных типов полей маппера """

    converters = {
        ("Int", "Int"): lambda v, mf, cache: int(v) if None != v else FNone(),
        ("Int", "String"): lambda v, mf, cache: str(v) if v else FNone(),
        ("Int", "Date"): lambda v, mf, cache: date.fromtimestamp(v) if v else FNone(),
        ("Int", "DateTime"): lambda v, mf, cache: datetime.fromtimestamp(v) if v else FNone(),
        ("Int", "Time"): lambda v, mf, cache: FieldTypesConverter.int2time(v) if v else FNone(),
        ("Int", "Bool"): lambda v, mf, cache: v != 0,
        ("Int", "Link"): lambda v, mf, cache: mf.get_new_item().load_by_primary(v, cache) if v else FNone(),
        ("String", "String"): lambda v, mf, cache: v.strip() if v else FNone(),
        ("String", "Int"): lambda v, mf, cache: int(v.strip()) if v else FNone(),
        ("String", "Float"): lambda v, mf, cache: float(v.strip()) if v else FNone(),
        ("String", "Date"): lambda v, mf, cache: FieldTypesConverter.str2date(v) if v else FNone(),
        ("String", "Time"): lambda v, mf, cache: FieldTypesConverter.str2time(v) if v else FNone(),
        ("String", "DateTime"): lambda v, mf, cache: FieldTypesConverter.str2datetime(v) if v else FNone(),
        ('String', 'Link'): lambda v, mf, cache: mf.get_new_item().load_by_primary(v, cache) if v else FNone(),
        ("String", "List"): lambda v, mf, cache: FieldTypesConverter.from_list_to_special_type_list(mf, v, cache),
        ("String", "ReversedLink"): lambda v, mf, cache:  FieldTypesConverter.to_reversed_link(mf, v, cache),
        ("Float", "Float"): lambda v, mf, cache: v if v else FNone(),
        ("Float", "String"): lambda v, mf, cache: str(v) if v else FNone(),
        ("Bool", "Bool"): lambda v, mf, cache: v if v else FNone(),
        ("Bool", "Int"): lambda v, mf, cache: 1 if v else 0,
        ("Date", "Date"): lambda v, mf, cache: v,
        ("Date", "Int"): lambda v, mf, cache: int(time.mktime(v.timetuple())) if v else FNone(),
        ("Date", "String"): lambda v, mf, cache: v.isoformat() if v else FNone(),
        ("Date", "DateTime"): lambda v, mf, cache: datetime(v.year, v.month, v.day) if v else FNone(),
        ("Time", "Time"): lambda v, mf, cache: v,
        ("Time", "Int"): lambda v, mf, cache: (v.hour * 3600 + v.minute*60 + v.second) if v else FNone(),
        ("Time", "String"): lambda v, mf, cache: v.strftime("%H:%M:%S") if v else FNone(),
        ("DateTime", "DateTime"): lambda v, mf, cache: v if v else FNone(),
        ("DateTime", "String"): lambda v, mf, cache: v.strftime("%Y-%m-%d %H:%M:%S") if v else FNone(),
        ("DateTime", "Int"): lambda v, mf, cache: int(time.mktime(v.timetuple())) if v else FNone(),
        ("DateTime", "Date"): lambda v, mf, cache: date(v.year, v.month, v.day) if v else FNone(),
        ("Link", "Int"): lambda v, mf, cache: v.save().get_primary_value() if v else FNone(),
        ("Link", "String"): lambda v, mf, cache: str(v.save().get_primary_value()) if v else FNone(),
        ("Link", "ObjectID"): lambda v, mf, cache: v.save().get_primary_value() if v else FNone(),
        ("List", "String"): lambda v, mf, cache: FieldTypesConverter.from_list_to_special_type_list(mf, v, cache),
        ("List", "ObjectID"): lambda v, mf, cache: [it.save().get_primary_value() for it in v] if v is not None else [],
        ("EmbeddedLink", "EmbeddedDocument"): lambda v, mf, cache: FieldTypesConverter.embedded(mf, v),
        ("EmbeddedDocument", "EmbeddedLink"): lambda v, mf, cache: FieldTypesConverter.from_embedded(mf, v),
        ("EmbeddedList", "EmbeddedDocument"): lambda v, mf, cache:
        [FieldTypesConverter.embedded(mf, i) for i in v] if v else [],
        ("EmbeddedDocument", "EmbeddedList"): lambda v, mf, cache:
        [FieldTypesConverter.from_embedded(mf, i) for i in v] if v else [],
        ("ObjectID", "List"): lambda v, mf, cache: FieldTypesConverter.from_list_to_special_type_list(mf, v, cache),
        ("ObjectID", "Link"): lambda v, mf, cache: mf.get_new_item().load_by_primary(v, cache) if v else FNone(),
        ("ObjectID", "ReversedLink"): lambda v, mf, cache: FieldTypesConverter.to_reversed_link(mf, v, cache),
        ("ObjectID", "ObjectID"): lambda v, mf, cache: v,
        ("Unknown", "String"): lambda v, mf, cache: str(v),
        ("Int", "Repr"): lambda v, mf, cache: str(v),
        ("String", "Repr"): lambda v, mf, cache: str(v),
        ("Float", "Repr"): lambda v, mf, cache: str(v),
        ("Bool", "Repr"): lambda v, mf, cache: str(v),
        ("Date", "Repr"): lambda v, mf, cache: str(v),
        ("Time", "Repr"): lambda v, mf, cache: str(v),
        ("DateTime", "Repr"): lambda v, mf, cache: str(v),
        ("DateTime", "Repr"): lambda v, mf, cache: str(v),
        ("Link", "Repr"): lambda v, mf, cache: str(v),
        ("List", "Repr"): lambda v, mf, cache: str(v),
        ("EmbeddedLink", "Repr"): lambda v, mf, cache: str(v),
        ("EmbeddedDocument", "Repr"): lambda v, mf, cache: str(v),
        ("EmbeddedList", "Repr"): lambda v, mf, cache: str(v),
        ("ObjectID", "Repr"): lambda v, mf, cache: str(v),
        ("Unknown", "Repr"): lambda v, mf, cache: str(v),
        ("EmbeddedObject", "Int"): lambda v, mf, cache: FieldTypesConverter.custom_types(v, mf, cache, "Int"),
        ("EmbeddedObject", "String"): lambda v, mf, cache: FieldTypesConverter.custom_types(v, mf, cache, "String"),
        ("EmbeddedObject", "Float"): lambda v, mf, cache: FieldTypesConverter.custom_types(v, mf, cache, "Float"),
        ("EmbeddedObject", "Bool"): lambda v, mf, cache: FieldTypesConverter.custom_types(v, mf, cache, "Bool"),
        ("EmbeddedObject", "Date"): lambda v, mf, cache: FieldTypesConverter.custom_types(v, mf, cache, "Date"),
        ("EmbeddedObject", "Time"): lambda v, mf, cache: FieldTypesConverter.custom_types(v, mf, cache, "Time"),
        ("EmbeddedObject", "DateTime"): lambda v, mf, cache: FieldTypesConverter.custom_types(v, mf, cache, "DateTime"),
        ("EmbeddedObject", "EmbeddedObject"):
        lambda v, mf, cache: None if not v else v.get_value() if isinstance(v, EmbeddedObject) else mf.model(v),
        ("Int", "EmbeddedObject"): lambda v, mf, cache: mf.model(v) if v else None,
        ("String", "EmbeddedObject"): lambda v, mf, cache: mf.model(v) if v else None
    }

    @staticmethod
    def str2date(value: str) -> date:
        """
        Конвертирует строковое представление даты к объекту даты
        @param value: Строковое представление даты
        @type value: str
        @return: Дата
        @rtype : date

        """
        value = datetime.strptime(value, "%Y-%m-%d")
        return date(value.year, value.month, value.day)

    @staticmethod
    def str2time(value: str) -> time:
        """
        Конвертирует строковое представление даты к объекту времени
        @param value: Строковое представление времени
        @type value: str
        @return: Дата
        @rtype : time

        """
        value = time.strptime(value, "%H:%M:%S")
        return dtime(value.tm_hour, value.tm_min, value.tm_sec)

    @staticmethod
    def str2datetime(value: str) -> datetime:
        """
        Конвертирует строковое представление даты к объекту даты и времени
        @param value: Строковое представление даты и времени
        @type value: str
        @return: Дата и время
        @rtype : datetime

        """
        value = time.strptime(value, "%Y-%m-%d %H:%M:%S")
        return datetime(value.tm_year, value.tm_mon, value.tm_mday, value.tm_hour, value.tm_min, value.tm_sec)

    @staticmethod
    def int2time(value: int) -> datetime:
        """
        Конвертирует число в объект времени, считая число количество секунд
        @param value: Количество секунд
        @type value: int
        @return: Дата и время
        @rtype : datetime

        """
        hours = int(value / 3600)
        minutes = int((value - (hours * 3600)) / 60)
        seconds = value - (hours * 3600) - (minutes * 60)
        return dtime(int(hours), int(minutes), int(seconds))

    @staticmethod
    def to_reversed_link(mf: FieldTypes.SqlReversedLink, v, cache) -> RecordModel:
        """
        Конвертирует значение в объект модели в соответствии с типом SqlReversedLink
        @param mf: Поле маппера
        @type mf: FieldTypes.BaseField
        @param v:  Значение для конвертации
        @param cache: Используемый кэш
        @return: Объект привязанной модели
        @rtype : RecordModel
        @raise TableMapperException: Если поле маппера сконфигурировано неправильно и представляет более одной записи

        """
        v = FieldTypesConverter.handle_none_value_for_list_types(v, mf)
        collection = list(filter(None, v))
        length = len(collection)
        if length > 1:
            raise TableMapperException(
                "SqlReversedLink field is configured incorrectly and represents more than one record"
            )
        elif length == 0:
            return None
        else:
            return mf.get_new_item().load_by_primary(collection[0], cache)

    @staticmethod
    def from_list_to_special_type_list(mf, v, cache):
        """
        Конвертирует значение из списка первичных ключей к списку моделей
        @param mf: Поле маппера
        @param v: Значение для конвертации
        @param cache: ИСпользуемый кэш
        @return: Список привязанных моделей
        @rtype : FieldValues.ListValue

        """
        v = FieldTypesConverter.handle_none_value_for_list_types(v, mf)
        if mf.get_items_collection_mapper().primary.compound:
            v = [
                dict(zip(mf.get_items_collection_mapper().primary.name(), val.split("$!")))
                for val in filter(lambda val: val not in [None, ""], v)
                if type(val) in [str, int]
            ]
        else:
            v = filter(None, v)
        unique = []
        for i in v:
            if i not in unique:
                unique.append(i)
        return FieldValues.ListValue([mf.get_new_item().load_by_primary(objid, cache) for objid in unique])

    @staticmethod
    def handle_none_value_for_list_types(v, mf):
        """
        Конвертирует возможные входные значения к формату полей List маппера, учитывая ситуации с None значениями...
        Нужно учитывать разные случаи:
        Например,
        Когда postgresql делает ARRAY_AGG(...) и значений нет - она возвращает сюда [None], а Mysql - просто None
        Когда mysql делает GROUP_CONCAT(...) она возвращает значения в виде строки, где все элементы соединены запятой
        @param v: Значения для конвертации
        @return: Сконвертированное значение
        """
        if v is None:
            v = [v]
        to_int = isinstance(mf.mapper.get_property(mf.mapper.primary.name()), FieldTypes.Int)
        if type(v) in [int, str]:
            if type(v) is str and v.find(",") > -1:
                v = [int(it) if it.isdigit() and to_int else it for it in v.split(",")]
            else:
                v = [int(v) if v.isdigit() and to_int else v]
        return v

    @staticmethod
    def embedded(mf, v):
        return mf.get_new_item().mapper.translate_and_convert(v.get_data()) if v else FNone()

    @staticmethod
    def from_embedded(mf, v):
        return mf.get_new_item().load_from_array(
            mf.get_new_item().mapper.translate_and_convert(v, "database2mapper"), True
        ) if v else FNone()

    @staticmethod
    def custom_types(v, mf, cache, target_type):
        if not v:
            return None
        target_lambda = FieldTypesConverter.converters.get(
            (mf.get_value_type_in_mapper_terms(v.get_value_type()).ident, target_type)
        )
        return target_lambda(v.get_value(), mf, cache)


# noinspection PyPep8Naming
def MapperMock(real_mapper):
    """
    Возвращает мок для переданного маппера
    @param real_mapper: Настоящий маппер
    @return: Мок маппера
    """

    from unittest.mock import Mock
    collection = real_mapper.get_new_collection()
    item = real_mapper.get_new_item()
    mapper_mock = Mock(spec=SqlMapper)
    mapper_mock.get_properties.return_value = real_mapper.get_properties()
    mapper_mock.split_data_by_relation_type.return_value = {}, {}
    mapper_mock.primary = Mock(exists=lambda: True)
    item.set_mapper(mapper_mock)
    collection.mapper = mapper_mock
    mapper_mock.get_new_collection.return_value = collection
    mapper_mock.get_new_item.return_value = item
    mapper_mock.insert.return_value = item
    return mapper_mock