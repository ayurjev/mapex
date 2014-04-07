
""" Модуль для работы с БД """

import hashlib
from abc import ABCMeta, abstractmethod
from mapex.core.Exceptions import TableModelException
from copy import deepcopy


class TableModel(object):
    """ Класс создания моделей таблиц БД """
    mapper = None

    def __init__(self, boundaries=None):
        if self.__class__.mapper is None:
            raise TableModelException("No mapper for %s model" % self)
        # noinspection PyCallingNonCallable
        self.object_boundaries = boundaries
        self.mapper = self.__class__.mapper()

    def get_new_item(self):
        """
        Возвращает тип, которому будут соответствовать все элементы, входящие в состав данной коллеции
        По умолчанию будет некий абстрактный тип TableRecordModel,
        то есть обычный RecordModel, но с маппером текущей модели
        :return: type
        """
        return self.mapper.get_new_item()

    def check_incoming_data(self, data):
        """
        Проверяет входящие данные на корректноcть
        :param data:    Входящие данные, которые требуется проверить
        """
        if type(data) is list:
            return [self.check_incoming_data(item) for item in data]
        else:
            if type(data) is dict:
                data = self.get_new_item().load_from_array(data, False)
            if isinstance(data, RecordModel):
                if data.mapper.__class__ != self.mapper.__class__:
                    raise TableModelException("Invalid data for inserting into the collection")
                data.validate()
                data_for_write_operation = data.get_data_for_write_operation()
                for mapper_field_name in data_for_write_operation:
                    mapper_field = self.mapper.get_property(mapper_field_name)
                    mapper_field.check_value(data_for_write_operation[mapper_field_name])
            else:
                raise TableModelException("Invalid data for inserting into the collection")
            return data

    def mix_boundaries(self, conditions: dict=None):
        if self.object_boundaries and conditions and self.mapper.boundaries:
            conditions = {"and": [conditions, self.object_boundaries, self.mapper.boundaries]}
        elif self.object_boundaries and self.mapper.boundaries:
            conditions = {"and": [self.object_boundaries, self.mapper.boundaries]}
        elif conditions and self.mapper.boundaries:
            conditions = {"and": [conditions, self.mapper.boundaries]}
        elif conditions and self.object_boundaries:
            conditions = {"and": [conditions, self.object_boundaries]}
        elif self.object_boundaries:
            conditions = self.object_boundaries
        elif self.mapper.boundaries:
            conditions = self.mapper.boundaries
        return conditions

    def count(self, conditions=None):
        """
        Выполняет подсчет объектов в коллекции по заданным условиям
        :param conditions: Условия подсчета строк в коллекции
        """
        return self.mapper.count(self.mix_boundaries(conditions))

    def insert(self, data):
        """
        Осуществляет вставку данных в коллекцию
        :param data:    Данные для вставки в коллекцию
        """
        new_item = lambda: self.get_new_item()
        data = self.check_incoming_data(data)
        if type(data) is list:
            return [self.insert(it) for it in data]

        if isinstance(data, dict):
            model = new_item()
            model.load_from_array(data)
            model_data = data
        elif isinstance(data, RecordModel):
            model_data = data.get_data_for_write_operation()
            model = data
        else:
            raise TableModelException("Insert failed: unknown item format")

        flat_data, lists_objects = self.mapper.split_data_by_relation_type(model_data)
        last_record = self.mapper.insert(flat_data)
        if self.mapper.primary.exists():
            model.set_primary_value(last_record)
            self.mapper.link_all_list_objects(
                lists_objects, model.load_from_array(model.get_data(), loaded_from_db=True)
            )
        return model

    def delete(self, conditions=None):
        """
        Удаляет записи из коллекции в соответствии с переданными условиями
        :param conditions:  Условия удаления записей из таблицы
        """
        return self.mapper.delete(self.mix_boundaries(conditions))

    def update(self, data, conditions=None):
        """ Обновляет записи в коллекции
        :param data:        Данные для обновления записей
        :param conditions:  Условия обновления записей в коллекции
        """
        return self.mapper.update(
            data, self.mix_boundaries(conditions), lambda: self.get_new_item()
        )

    def get_property_list(self, property_name: str, conditions=None, params=None):
        """
        Возврашает список значений какого-либо свойства текущей коллекции в соответствии с уловиями и
        параметрами выборки
        :param property_name:       Имя требуемого поля
        :param conditions:          Условия выборки записей
        :param params:              Параметры выборки записей
        """
        for item in self.mapper.get_column(property_name, self.mix_boundaries(conditions), params):
            yield item

    def get_properties_list(self, properties_names: list, conditions=None, params=None):
        """
        Возвращает список, где каждый элемент состоит из значений всех запрошенных полей
        :param properties_names:     Список запрошенных полей
        :param conditions:          Условия выборки записей
        :param params:              Параметры выборки записей
        """
        for item in self.mapper.get_rows(properties_names, self.mix_boundaries(conditions), params):
            yield item

    def get_item(self, unique_bounds):
        """
        Возвращает инстанс одной модели из текущей коллекции в соответствии с условиями выборки
        :param unique_bounds:       Условия получения записи
        :return: RecordModel:       Экземпляр записи
        """
        items = self.get_items(unique_bounds)
        length = len(items)
        if length > 1:
            raise TableModelException("there is more than one item affected by get_item() method")
        return items[0] if length > 0 else None

    def get_items(self, bounds=None, params=None):
        """
        Возвращает список экзепляров класса RecordModel, соответствующих условиями выборки из коллекции
        :param bounds:              Условия выборки записей
        :param params:              Параметры выборки (сортировка, лимит)
        :return: :raise:            TableModelException
        """
        cache = TableModelCache(self.mapper)
        generator = self.mapper.get_rows([], self.mix_boundaries(bounds), params, cache)
        if isinstance(self.get_new_item().mapper, self.mapper.__class__) is False:
            raise TableModelException("Collection mapper and collection item mapper should be equal")
        arrays, items = [], []
        for row in generator:
            arrays.append(row)
            item = self.mapper.factory_method(self.get_new_item().load_from_array(row, True))
            items.append(item)
        cache.cache(arrays)
        return items


class OriginModel(object):
    def __init__(self, data):
        self.__dict__ = data


class RecordModel(object):
    """ Класс создания моделей записей в таблицах БД """
    mapper = None

    def __init__(self, data=None, loaded_from_db=False):
        self._lazy_load = None
        self._loaded_from_db = False
        self._collection = None
        self.md5_data = {}
        self.md5 = None
        self.origin = None
        if self.__class__.mapper is None:
            raise TableModelException("No mapper for %s model" % self)
        # noinspection PyCallingNonCallable
        self.set_mapper(self.__class__.mapper(), data, loaded_from_db)

    def set_mapper(self, mapper, data=None, loaded_from_db=False):
        if mapper:
            self.mapper = mapper
            self._collection = self.get_new_collection()
            current_data = {
                property_name: self.mapper.get_property(property_name).get_default_value()
                for property_name in self.mapper.get_properties()
            }
            self.load_from_array(current_data)
            self.md5 = self.calc_sum()
            if data:
                self.load_from_array(data.get_data() if isinstance(data, RecordModel) else data, loaded_from_db)

    # noinspection PyMethodMayBeStatic
    def validate(self):
        """ Данный метод можно переопределить для автоматической проверки модели перед записью в базу данных """

    def get_new_collection(self) -> TableModel:
        """
        Возвращает модель коллекции, соответствующую мапперу текущего объекта

        @return: Модель коллекции
        @rtype : TableModel
        """
        return self.mapper.get_new_collection()

    def save(self):
        """ Сохраняет текущее состояние модели в БД """
        if self.mapper.primary.exists() is False:
            raise TableModelException("there is no primary key for this model, so method save() is not allowed")
        self.validate()
        data_for_insert = self.get_data_for_write_operation()
        if self._loaded_from_db is False:
            res = self._collection.insert(data_for_insert)
            self.__dict__ = res.__dict__
            self.md5 = self.calc_sum()
            self.origin = OriginModel(self.get_data())
            self.set_primary_value(self.get_actual_primary_value())
            return self
        else:
            # Если объект загружен из БД и его сумма не изменилась, то просто отдаем primary
            if self.md5 == self.calc_sum():
                return self
            self.md5 = self.calc_sum()  # пересчет md5 должен происходить до Update, чтобы избежать рекурсии
            self._collection.update(data_for_insert, self.mapper.primary.eq_condition(self.get_old_primary_value()))
            self.origin = OriginModel(self.get_data())
            self.set_primary_value(self.get_actual_primary_value())
            return self

    def remove(self):
        """ Удаляет объект из коллекции """
        if self.mapper.primary.exists() is False:
            raise TableModelException("there is no primary key for this model, so method remove() is not allowed")
        self._collection.delete(self.mapper.primary.eq_condition(self.get_old_primary_value()))

    def refresh(self):
        """ Обновляет состояние модели в соответствии с состоянием в БД """
        if self.mapper.primary.exists() is False:
            raise TableModelException("there is no primary key for this model, so method refresh() is not allowed")
        self.load_from_array(self.get_new_collection().get_item(
            self.mapper.primary.eq_condition(self.get_actual_primary_value())
        ).get_data(), loaded_from_db=True)

    def load_by_primary(self, primary, cache=None):
        """
        Выполняет отложенную инициализацю объекта по значению первичного ключа.
        Реально наполнения данными объекта не происходит, создается лишь заготовка для наполнения,
        которая будет выполнена при первом требовании
        :param primary:    Значение первичного ключа записи
        :param cache:            Кэш
        :return: self
        """
        if self.mapper.primary.exists() is False:
            raise TableModelException(
                "there is no primary key defined for that model type, so load_by_primary() is not allowed"
            )

        if not self.mapper.primary.compound:
            primary_mf = self.mapper.get_property(self.mapper.primary.name())
            if self.mapper.is_embedded_object(primary_mf):
                primary = primary_mf.model(primary)

        self.set_primary_value(primary)
        self._lazy_load = lambda: self.cache_load(cache) if cache else lambda: self.normal_load()
        return self

    def load_from_array(self, data, loaded_from_db=False):
        """
        Инициализирует объект данными из словаря
        :param data:    Словарь с данными
        """
        self._loaded_from_db = loaded_from_db
        for key in data:
            self.__setattr__(key, data[key])
        if loaded_from_db:
            self.md5 = self.calc_sum()
            self.origin = OriginModel(data)
        return self

    def normal_load(self):
        """
        Выполняет обычную инициализацию объекта (с помощью запросов к БД)
        @return Ссылка на текущую модель
        @rtype : RecordModel

        """
        data = self.mapper.get_row([], self.mapper.primary.eq_condition(self.get_actual_primary_value()))
        return self.load_from_array(data, True) if data else None

    def cache_load(self, cache):
        """
        Выполняет инициализацию с помощью кэша
        @param cache: Кэш
        @return Ссылка на текущую модель
        @rtype : RecordModel

        """
        data = cache.get(self.mapper, self.get_actual_primary_value())
        return self.load_from_array(data, True) if data else None

    def exec_lazy_loading(self):
        """ Если объект проиницилиазирован отложенно - вызывает инициализацию """
        lazy = self._lazy_load
        if lazy:
            self._lazy_load = False
            # noinspection PyCallingNonCallable
            res = lazy()
            return res

    def set_primary_value(self, primary_value):
        """
        Устанавливает новое значение первичного ключа для текущей модели
        @param primary_value: Значение первичного ключа
        @return: Установленное значение первичного ключа

        """
        self._loaded_from_db = primary_value
        if type(primary_value) is dict:
            self.__dict__.update(primary_value)
        else:
            self.__dict__[self.mapper.primary.name()] = primary_value
        return primary_value

    def unset_primary(self):
        """ Стирает из модели значение первичного ключа """
        actual_primary = self.get_actual_primary_value()
        if type(actual_primary) is dict:
            self.set_primary_value({key: self.mapper.get_base_none() for key in actual_primary})
        else:
            self.set_primary_value(self.mapper.get_base_none())

    def get_actual_primary_value(self):
        """
        Возвращает текущее (актуальное) значение первичного ключа для модели
        @return: Значение первичного ключа

        """
        return self.mapper.primary.grab_value_from(self.__dict__)

    def get_primary_value(self):
        """ Алиас для get_actual_primary_value() """
        return self.get_actual_primary_value()

    def get_old_primary_value(self):
        """
        Возвращает значение первичного ключа, которое было при изначальной загрузке модели, то есть то, что в есть в бд
        @return: Значение первичного ключа

        """
        return self._loaded_from_db if type(self._loaded_from_db) is not bool else self.get_actual_primary_value()

    def get_data(self, properties: list=None) -> dict:
        """
        Возвращает данные модели в виде словаря
        @param properties: Список полей (по умолчанию - все поля)
        @type properties: list
        @return: Словарь с данными модели
        @rtype : dict

        """
        self.exec_lazy_loading()
        mapper_properties = self.mapper.get_properties()
        properties = list(set(mapper_properties) & set(properties)) if properties else mapper_properties
        return {property_name: self.__dict__.get(property_name) for property_name in properties}

    def stringify(self, properties: list=None, stringify_depth: tuple=None, additional_data: dict=None) -> dict:
        """
        Возвращает подготовленное для шаблонизатора представление модели.
        @param properties: Список полей основной модели, которые требуется получить
        @type properties: list
        @param stringify_depth: Глубина преобразования (Текущий уровень (для итерации), Максимальный уровень)
        @type stringify_depth: tuple
        @param additional_data: Данные, которые необходимо добавить к полученному словарю
        @type additional_data: dict
        @return: Подготовленное к использованию в шаблонизаторе представление модели
        @rtype : dict
        
        """
        data = {}
        stringify_depth, stringify_level_max = stringify_depth if stringify_depth else (0, 1)
        embeded_models_properties = {}
        if type(properties) is tuple:
            properties, embeded_models_properties = properties

        for property_name in self.get_data(properties):
            value = self.__dict__[property_name]
            if isinstance(value, RecordModel):
                if stringify_depth < stringify_level_max:
                    value = value.stringify(
                        embeded_models_properties.get(property_name),
                        stringify_depth=(stringify_depth + 1, stringify_level_max)
                    )
                else:
                    value = None
            elif isinstance(value, list):
                value = [
                    item.stringify(
                        embeded_models_properties.get(property_name),
                        stringify_depth=(stringify_depth + 1, stringify_level_max)
                    )
                    if isinstance(item, RecordModel) and stringify_depth < stringify_level_max else (
                        None if isinstance(item, RecordModel) else item
                    ) for item in value
                ]
            data[property_name] = value
        if type(additional_data) is dict:
            for additional_key in additional_data:
                if properties is None or additional_key in properties:
                    data[additional_key] = additional_data[additional_key]
        return data

    def get_data_for_write_operation(self) -> dict:
        """
        Возвращает данные модели, которые могли бы быть использованы при вставке записи
        Эти данные должны уникально идентифицировать объект. Пустые (незаполненные) поля отбрасываются
        @return Словарь с данными для записи в БД
        @rtype : dict

        """
        data_for_insert = {}
        all_data = self.get_data()
        for key in all_data:
            if (
                    self.mapper.is_base_value(all_data[key]) is False or
                    self.md5_data.get(key) != hashlib.md5(str(all_data[key]).encode()).hexdigest() or
                    all_data[key].changed
            ):
                data_for_insert[key] = all_data[key]
        return data_for_insert

    def calc_sum(self) -> str:
        """
        Считает хэш-сумму для модели
        @return: Хэш-сумма данных модели
        @rtype : str

        """
        all_data = self.get_data()
        self.md5_data = {key: hashlib.md5(str(all_data[key]).encode()).hexdigest() for key in all_data}
        return hashlib.md5(str(self.get_data()).encode()).hexdigest()

    def __setattr__(self, name, val):
        """ При любом изменении полей модели необходимо инициализировать модель """
        mapper = object.__getattribute__(self, "__dict__").get("mapper")
        if mapper and name in mapper.get_properties():
            self.exec_lazy_loading()
        object.__setattr__(self, name, val)

    def __getattribute__(self, name):
        """ При любом обращении к полям модели необходимо инициализировать модель """
        mapper = object.__getattribute__(self, "__dict__").get("mapper")
        if mapper and name in mapper.get_properties():
            self.exec_lazy_loading()
        return object.__getattribute__(self, name)

    def __eq__(self, other):
        """
        Переопределяем метод сравнения двух экземпляров класса
        @param other: Второй экземпляр класса
        @return: Результат сравнения

        """
        if isinstance(other, RecordModel):
            first = self.get_actual_primary_value()
            second = other.get_actual_primary_value()
            return first == second if first and second else self.get_data() == other.get_data()
        else:
            return False


class EmbeddedObject(object, metaclass=ABCMeta):
    """
    Класс для создания моделей, для которых в БД может храниться только одно значение,
    на основе которого должно происходить конструирование экземпляров класса этой модели
    """
    @abstractmethod
    def get_value(self):
        """
        @return: Значение, которое будет храниться в поле таблицы БД
        и на основе которого будет конструироваться экземпляр данного класса
        """

    @staticmethod
    @abstractmethod
    def get_value_type():
        """
        @return: Тип значения, которое будует храниться в БД в качестве идентификатора данной модели
        """

    def __eq__(self, other):
        if isinstance(other, EmbeddedObject):
            return self.get_value() == other.get_value()


class EmbeddedObjectFactory(object, metaclass=ABCMeta):

    def __new__(cls, value):
        return cls.get_instance(value)

    @classmethod
    def get_instance(cls, value):
        """
        @param value: Значение, для конструирования экземпляра класса
        @return: Возвращает созданный экземпляр, корректного для данного value класса
        """
        raise NotImplementedError("method get_instance of EmbeddedObjectFactory is not implemented")


class TableModelCache(object):
    """ Класс для кэширования моделей уже проинициализированных моделей """
    def __init__(self, mapper):
        self._mapper = mapper
        self._cache = {}

    def get(self, model_type, primary_id):
        """
        Возвращает данные для модели типа model_type, первичный ключ которой равен primary_id
        :param model_type:   Тип модели
        :param primary_id:   Значение первичного ключа
        """
        model_cache = self._cache.get(model_type)
        if model_cache:
            if type(model_cache) is not dict:
                self._cache[model_type] = self._cache[model_type](model_type)
                return self._cache[model_type].get(primary_id.get_value() if isinstance(primary_id, EmbeddedObject) else primary_id)
            return model_cache.get(primary_id)

    def cache(self, rows):
        """
        Выполняет кэширование для будущего использования
        :param rows:    Список строк для кэширования
        """
        # Сперва получим имена итересующих нас полей (Кэшируются только данные для полей Link и List)
        cache = {}
        res_cache = {}
        field_names_for_cache = {}
        for field_name in self._mapper.get_properties():
            mf = self._mapper.get_property(field_name)
            if mf and self._mapper.is_rel(mf):
                field_names_for_cache[field_name] = {"mapper": mf.get_items_collection_mapper(), "mapper_field": mf}
                cache[mf.get_items_collection_mapper()] = []
                res_cache[mf.get_items_collection_mapper()] = {}
        for row in rows:
            for field_name in field_names_for_cache.keys():
                if None != row.get(field_name):
                    if self._mapper.is_link(field_names_for_cache[field_name]["mapper_field"]):
                        cache[field_names_for_cache[field_name]["mapper"]].append(
                            row[field_name].get_actual_primary_value().get_value() if isinstance(row[field_name].get_actual_primary_value(), EmbeddedObject) else row[field_name].get_actual_primary_value()
                        )
                    elif self._mapper.is_reversed_link(field_names_for_cache[field_name]["mapper_field"]):
                        cache[field_names_for_cache[field_name]["mapper"]].append(
                            row[field_name].get_actual_primary_value()
                        )
                    elif self._mapper.is_list(field_names_for_cache[field_name]["mapper_field"]):
                        for obj in row[field_name]:
                            cache[field_names_for_cache[field_name]["mapper"]].append(obj.get_actual_primary_value())

        def _get_mapper_cache(m):
            """ Собирает кэш маппера из внешней переменной cache """
            mapper_cache = {}
            for item in m.get_new_collection().get_items({m.primary.name(): ("in", cache[m])}):
                key = item.get_actual_primary_value()
                if isinstance(key, EmbeddedObject):
                    primary_value = key.get_value()
                elif isinstance(key, RecordModel):
                    primary_value = key.get_actual_primary_value()
                else:
                    primary_value = key
                mapper_cache[primary_value] = item.get_data()
            return mapper_cache

        for mapper in cache:
            if len(cache[mapper]) > 0:
                self._cache[mapper] = _get_mapper_cache
