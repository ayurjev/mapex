
""" Модуль для работы с БД """

from abc import ABCMeta
from mapex.src.Exceptions import TableModelException, EmbeddedObjectFactoryException
from mapex.src.Common import TrackChangesValue, ValueInside
from mapex.src.Utils import do_dict, merge_dict
from collections import OrderedDict
import weakref
import re
import json
import base64


class TableModel(object):
    """ Класс создания моделей таблиц БД """
    mapper = None

    def __init__(self, *boundaries, pool=None):
        self._pool = pool
        if self.__class__.mapper is None:
            raise TableModelException("No mapper for %s model" % self)
        # noinspection PyCallingNonCallable
        self.object_boundaries = None
        if boundaries and len(boundaries) > 0:
            self.object_boundaries = {}
            for b in boundaries:
                if b:
                    self.object_boundaries.update(b)
        # noinspection PyCallingNonCallable
        self.mapper = self.__class__.mapper()

    @property
    def pool(self):
        return self._pool or self.mapper.pool

    @pool.setter
    def pool(self, pool):
        self._pool = pool

    def get_new_item(self, data=None):
        """
        Возвращает тип, которому будут соответствовать все элементы, входящие в состав данной коллеции
        По умолчанию будет некий абстрактный тип TableRecordModel,
        то есть обычный RecordModel, но с маппером текущей модели
        :return: type
        """
        return self.mapper.get_new_item(data, self.pool)

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

    def get_bounds(self):
        """ Возвращает все известыне объекту ограничения выборки """
        return self.mix_boundaries()

    def count(self, conditions=None):
        """
        Выполняет подсчет объектов в коллекции по заданным условиям
        :param conditions: Условия подсчета строк в коллекции
        """
        return self.mapper.count(self.mix_boundaries(conditions), self.pool)

    def check_incoming_data(self, data):
        """
        Проверяет входящие данные на корректноcть
        :param data:    Входящие данные, которые требуется проверить
        """
        if type(data) is list:
            for item in data:
                self.check_incoming_data(item)
        else:
            if isinstance(data, RecordModel) and data.mapper.__class__ == self.mapper.__class__:
                data.validate()
            else:
                raise TableModelException("Invalid data for inserting into the collection")

    def insert(self, data):
        """
        Осуществляет вставку данных в коллекцию
        :param data:    Данные для вставки в коллекцию
        """
        self.check_incoming_data(data)
        return [self._insert_one(item) for item in data] if (type(data) is list) else self._insert_one(data)

    def _insert_one(self, item):
        """ Вставка записи без выполнения проверок """
        model_data = item.get_data_for_write_operation()
        model = item

        flat_data, lists_objects = self.mapper.split_data_by_relation_type(model_data)
        last_record = self.mapper.insert(flat_data, model_pool=self.pool)
        if self.mapper.primary.exists():
            model.primary.set_value(last_record)
            with UpdateLock(model):
                self.mapper.link_all_list_objects(
                    lists_objects, model.load_from_array(model.get_data(), consider_as_unchanged=True), model_pool=self.pool
                )
                model.up_to_date()
        return model

    def delete(self, conditions=None):
        """
        Удаляет записи из коллекции в соответствии с переданными условиями
        :param conditions:  Условия удаления записей из таблицы
        """
        return self.mapper.delete(self.mix_boundaries(conditions), model_pool=self.pool)

    def update(self, data, conditions=None, params=None, model=None):
        """ Обновляет записи в коллекции
        :param data:        Данные для обновления записей
        :param conditions:  Условия обновления записей в коллекции
        :param params:      Параметры обновления записей в коллекции
        """
        flat_data, lists_objects = self.mapper.split_data_by_relation_type(data)
        # Отсекаем из массива изменений, то,
        # что в неизменном виде присутствует в массиве условий (то есть не будет изменено)
        # Это важно особенно, так как в flat_data не должны попадать те значения первичных ключей, которые не изменялись
        # Так как они могут быть что-то вроде IDENTITY полей и не подлежат изменениям даже на теже самые значения
        conditions = self.mix_boundaries(conditions)
        if conditions:
            if self.mapper.primary.exists() and not self.mapper.primary.compound:
                primary_name = self.mapper.primary.name()
                primary_in_conditions = conditions.get(primary_name)
                primary_in_flat_data = flat_data.get(primary_name)
                if isinstance(primary_in_conditions, RecordModel) and primary_in_conditions == primary_in_flat_data:
                    primary_in_flat_data.save()
                flat_data = {key: flat_data[key] for key in flat_data if conditions.get(key, "&bzx") != flat_data[key]}

        # Сохраняем записи в основной таблице
        changed_models_pkeys = self.mapper.update(flat_data, conditions, params, model_pool=self.pool)

        if len(changed_models_pkeys) > 0 and lists_objects != {}:
            if model:
                items_to_update = [model]
            elif self.mapper.primary.compound:
                items_to_update = [self.get_item(compound_primary) for compound_primary in changed_models_pkeys]
            else:
                pkeys_raw = [pk.get_value() if isinstance(pk, ValueInside) else pk for pk in changed_models_pkeys]
                items_to_update = self.get_items({self.mapper.primary.name(): ("in", pkeys_raw)})
            for updated_item in items_to_update:
                self.mapper.link_all_list_objects(lists_objects, updated_item, self.pool)

    def get_property_list(self, property_name: str, conditions=None, params=None):
        """
        Возврашает список значений какого-либо свойства текущей коллекции в соответствии с уловиями и
        параметрами выборки
        :param property_name:       Имя требуемого поля
        :param conditions:          Условия выборки записей
        :param params:              Параметры выборки записей
        """
        for item in self.mapper.get_column(property_name, self.mix_boundaries(conditions), params, model_pool=self.pool):
            yield item

    def get_properties_list(self, properties_names: list, conditions=None, params=None):
        """
        Возвращает список, где каждый элемент состоит из значений всех запрошенных полей
        :param properties_names:     Список запрошенных полей
        :param conditions:          Условия выборки записей
        :param params:              Параметры выборки записей
        """
        for item in self.mapper.get_rows(properties_names, self.mix_boundaries(conditions), params, model_pool=self.pool):
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
        cache = TableModelCache(self.mapper, self.pool)
        generator = self.mapper.get_rows([], self.mix_boundaries(bounds), params, cache, model_pool=self.pool)
        if isinstance(self.get_new_item().mapper, self.mapper.__class__) is False:
            raise TableModelException("Collection mapper and collection item mapper should be equal")
        arrays, items = [], []
        for row in generator:
            arrays.append(row)
            item = self.mapper.factory_method(self.get_new_item().load_from_array(row, consider_as_unchanged=True))
            items.append(item)
        cache.cache(arrays)
        return items

    def pretty_print(self, bounds=None, params=None, properties=None, bytes_len=10):
        """
        Pretty print collection items
        :param bounds:     Условия выборки записей
        :param params:     Параметры выборки (сортировка, лимит)
        :param properties  Поля которые надо отобразить. Если не заполнено то отобразятся все
        :param bytes_len   Максимальная длина поля для bytes
        """
        from prettytable import PrettyTable

        if not properties:
            properties = self.mapper.get_properties()
        properties = sorted(properties)

        # Наполнение таблицы
        table = PrettyTable()
        for item in self.get_items(bounds, params):
            data = item.stringify(properties)

            # Имена колонок известны только теперь
            # т.к. .stringify схлопывает свойства подмодели в одно поле  по имени подмодели
            if not table.field_names:
                table.field_names = data.keys()

            for key, value in data.items():
                data[key] = value[:bytes_len] if isinstance(value, bytes) and bytes_len is not None else str(value)

            table.add_row(data.values())

        # Если в таблице нет данных то выводим пустую
        if not table.field_names:
            table.field_names = properties

        # Печать таблицы
        table.align = 'l'
        print(str(self.__class__) + '\n' + str(table))

    def generate_items(self, bounds=None, params=None):
        """
        Генератор экзепляров класса RecordModel, соответствующих условиями выборки из коллекции
        :param bounds:              Условия выборки записей
        :param params:              Параметры выборки (сортировка, лимит)
        :return: :raise:            TableModelException
        """
        if isinstance(self.get_new_item().mapper, self.mapper.__class__) is False:
            raise TableModelException("Collection mapper and collection item mapper should be equal")

        for row in self.mapper.generate_rows([], self.mix_boundaries(bounds), params, model_pool=self.pool):
            yield self.mapper.factory_method(self.get_new_item().load_from_array(row, consider_as_unchanged=True))


class Primary(ValueInside):
    def __init__(self, model):
        self.model = model
        self.pk_list = None

    def to_dict(self, origin=False):
        return self.model.mapper.primary.eq_condition(self.origin if origin else self.value)

    @property
    def origin(self):
        return self.model.mapper.primary.grab_value_from(self.model.origin.__dict__) \
            if self.model.origin else self.get_value()

    @property
    def value(self):
        return self.get_value()

    def get_value(self, deep=False):
        value = self.model.mapper.primary.grab_value_from(self.model.__dict__)
        if deep:
            if isinstance(value, ValueInside):
                value = value.get_value(deep)
            elif isinstance(value, dict):
                value = {key: value[key].get_value(deep) if isinstance(value[key], ValueInside) else value[key] for key in value}
        return value

    def set_value(self, value):
        """
        Устанавливает новое значение первичного ключа для текущей модели
        @param value: Значение первичного ключа
        @return: Установленное значение первичного ключа
        """
        def try_to_create_model(mp, raw_value):
            primary_mf = self.model.mapper.get_property(mp)
            if self.model.mapper.is_embedded_object(primary_mf):
                raw_value = primary_mf.model(raw_value) if not isinstance(raw_value, EmbeddedObject) else raw_value
            if self.model.mapper.is_rel(primary_mf) and not isinstance(raw_value, RecordModel):
                raw_value = primary_mf.get_new_item(self.model.pool).load_by_primary(raw_value)

            raw_value = primary_mf.cast_to_field_type(raw_value, self.model.pool)
            return raw_value

        if not self.model.mapper.primary.compound:
            value = try_to_create_model(self.model.mapper.primary.name(), value)
        else:
            for key in value:
                value[key] = try_to_create_model(key, value[key])

        if type(value) is dict:
            self.model.__dict__.update(value)
        else:
            self.model.__dict__[self.model.mapper.primary.name()] = value
        return value

    def to_list(self):
        if self.pk_list is None:
            if not self.model.mapper or not self.model.mapper.primary.exists():
                self.pk_list = []
            elif self.model.mapper.primary.compound:
                self.pk_list = self.model.mapper.primary.name()
            else:
                self.pk_list = [self.model.mapper.primary.name()]
        return self.pk_list

    def ensure_exists(self):
        if self.model.mapper.primary.exists() is False:
            raise TableModelException("there is no primary key for %s model" % self.model)


class OriginModel(object):
    def __init__(self, data):
        self.__dict__ = data

    def get(self, key):
        """
        Возвращает значение из origin по имени ключа
        @param key: Имя свойства
        @return: Значения свойства в origin
        """
        return self.__dict__.get(key)


class RecordModelLock(object):
    flag = None

    def __init__(self, model):
        self.model = model

    def __enter__(self):
        self.model.__setattr__(self.flag, True)
        return self

    # noinspection PyUnusedLocal
    def __exit__(self, etype, value, traceback):
        self.model.__setattr__(self.flag, False)


class UpdateLock(RecordModelLock):
    """ Лок для выполнения операции обновления модели """
    flag = "_updating"


# noinspection PyDocstring
class CalcChangesLock(RecordModelLock):
    flag = "cant_calc_changed"


# noinspection PyDocstring
class ValidateLock(RecordModelLock):
    """ Лок для выполнения операции валидации модели """
    flag = "_validate_lock"


class RecordModel(ValueInside, TrackChangesValue):
    """ Класс создания моделей записей в таблицах БД """
    mapper = None

    def __init__(self, data=None, loaded_from_db=False, pool=None):
        self._pool = pool
        self._lazy_load = False
        self._changed = True
        # weakref.proxy решает проблему циклической связанности между экземплярами Primary и RecordModel
        self.primary = Primary(weakref.proxy(self))
        self.cant_calc_changed = False
        self._loaded_from_db = False
        self._updating = False
        self._validate_lock = False
        self.origin = None
        if self.__class__.mapper is None:
            raise TableModelException("No mapper for %s model" % self)
        # noinspection PyCallingNonCallable
        self.set_mapper(self.__class__.mapper())
        if data:
            self.load_from_array(data.get_data() if isinstance(data, RecordModel) else data, consider_as_unchanged=loaded_from_db)

    @property
    def pool(self):
        return self._pool or self.mapper.pool

    @pool.setter
    def pool(self, pool):
        self._pool = pool

    def get_value(self, deep=False):
        return self.primary.get_value(deep)

    def set_mapper(self, mapper):
        if mapper:
            self.mapper = mapper
            default_data = {
                property_name: self.mapper.get_property(property_name).get_default_value()
                for property_name in self.mapper.get_properties()
            }
            default_data.update(self.__dict__)
            self.__dict__ = default_data

    # noinspection PyMethodMayBeStatic
    def validate(self):
        """ Данный метод можно переопределить для автоматической проверки модели перед записью в базу данных """

    def recursive_validate(self):
        """ Рекурсивная валидация моделей """
        if not self._validate_lock:
            with ValidateLock(self):
                for model in self.values(lambda value: isinstance(value, RecordModel) and value.is_changed()):
                    model.validate()

                data_for_write_operation = self.get_data_for_write_operation()
                for mapper_field_name in data_for_write_operation:
                    mapper_field = self.mapper.get_property(mapper_field_name)
                    mapper_field.check_value(data_for_write_operation[mapper_field_name])

                object.__getattribute__(self, "validate")()

    def get_new_collection(self) -> TableModel:
        """
        Возвращает модель коллекции, соответствующую мапперу текущего объекта

        @return: Модель коллекции
        @rtype : TableModel
        """
        return self.mapper.get_new_collection(self.pool)

    def save(self):
        """ Сохраняет текущее состояние модели в БД """
        if self.mapper.is_mock:
            self.up_to_date()
            return self

        self.primary.ensure_exists()
        if not self._loaded_from_db:
            # ИБО НЕХ!
            # if self.get_value(deep=True) and self.mapper.primary.autoincremented and self.get_new_collection().count(self.primary.to_dict(origin=True)):
            #     # Если объект уже находится в состоянии сохранения или объект не был изменен
            #     # то выходим, чтобы разорвать рекурсию
            #     if self._updating or not self.is_changed():
            #         return self
            #
            #     self.validate()
            #     with UpdateLock(self):
            #         to_be_written = self.get_data_for_write_operation()
            #         self.get_new_collection().update(to_be_written, self.primary.to_dict(origin=True), model=self)
            #         self.up_to_date()
            #     return self
            try:
                self._loaded_from_db = True
                self.get_new_collection().insert(self)
                self.up_to_date()
                return self
            except Exception as err:
                self._loaded_from_db = False
                raise err
        else:
            # Если объект уже находится в состоянии сохранения или объект не был изменен
            # то выходим, чтобы разорвать рекурсию
            if self._updating or not self.is_changed():
                return self

            self.validate()
            with UpdateLock(self):
                to_be_written = self.get_data_for_write_operation()
                self.get_new_collection().update(to_be_written, self.primary.to_dict(origin=True), model=self)
                self.up_to_date()
        return self

    def remove(self):
        """ Удаляет объект из коллекции """
        self.primary.ensure_exists()
        self.get_new_collection().delete(self.primary.to_dict(origin=True))

    def refresh(self):
        """ Обновляет состояние модели в соответствии с состоянием в БД """
        self.primary.ensure_exists()
        self.mapper.refresh(self, model_pool=self.pool)

    def up_to_date(self):
        """ Пересоздает объект Origin для модели """
        for property_name in self.mapper.get_properties():
            if self.mapper.is_list(property_name) and type(self.__dict__.get(property_name)) is list:
                self.__dict__[property_name] = self.mapper.convert_to_list_value(self.__dict__[property_name])
            elif self.__dict__[property_name] is None:
                self.__dict__[property_name] = self.mapper.get_base_none()
        self.origin = OriginModel(self.get_data())
        self._changed = False

    def mark_as_changed(self):
        self._changed = True

    def load_by_primary(self, primary, cache=None):
        """
        Выполняет отложенную инициализацю объекта по значению первичного ключа.
        Реально наполнения данными объекта не происходит, создается лишь заготовка для наполнения,
        которая будет выполнена при первом требовании
        :param primary:    Значение первичного ключа записи
        :param cache:      Кэш
        :return: self
        """
        self.primary.ensure_exists()
        self.primary.set_value(primary)
        self._lazy_load = (lambda: self.cache_load(cache)) if cache else (lambda: self.normal_load())
        self._loaded_from_db = True
        self._changed = False
        return self

    def load_from_array(self, data, consider_as_unchanged=None):
        """
        Инициализирует объект данными из словаря
        :param data:    Словарь с данными
        """
        for key in data:
            self.__setattr__(key, data[key])

        if self.mapper.primary.exists():
            self.primary.set_value(self.primary.get_value())

        if consider_as_unchanged:
            self._loaded_from_db = True
            self.up_to_date()
        else:
            self.mark_as_changed()
        return self

    def set_primary(self, value):
        self.primary.set_value(value)
        self._loaded_from_db = True

    def normal_load(self):
        """
        Выполняет обычную инициализацию объекта (с помощью запросов к БД)
        @return Ссылка на текущую модель
        @rtype : RecordModel

        """
        data = self.mapper.get_row([], self.primary.to_dict(), model_pool=self.pool)
        # Первичный ключ не перезагружается чтобы не потерять изменения если ключ - это модель другой коллекции
        if data and self.mapper.primary.exists():
            if self.mapper.primary.compound:
                for pkey in self.mapper.primary.name():
                    del data[pkey]
            elif self.mapper.primary.name() in data:
                del data[self.mapper.primary.name()]
        return self.load_from_array(data, consider_as_unchanged=True) if data else None

    def cache_load(self, cache):
        """
        Выполняет инициализацию с помощью кэша
        @param cache: Кэш
        @return Ссылка на текущую модель
        @rtype : RecordModel

        """
        data = cache.get(self.mapper, self.primary.get_value(deep=True))

        # Первичный ключ не перезагружается чтобы не потерять изменения если ключ - это модель другой коллекции
        if data and self.mapper.primary.exists():
            if self.mapper.primary.compound:
                for pkey in self.mapper.primary.name():
                    del data[pkey]
            elif self.mapper.primary.name() in data:
                del data[self.mapper.primary.name()]
        return self.load_from_array(data, consider_as_unchanged=True) if data else None

    def exec_lazy_loading(self):
        """ Если объект проиницилиазирован отложенно - вызывает инициализацию """
        lazy, self._lazy_load = self._lazy_load, False
        return lazy() if lazy else None

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

    def stringify(self, properties: list=None) -> dict:
        """
        Возвращает подготовленное для шаблонизатора представление модели.
        @param properties: Список полей основной модели, которые требуется получить
        @type properties: list
        @rtype : dict
        """
        if not properties or not len(properties):
            return OrderedDict()
        dicts = []
        for prop in properties:
            if isinstance(prop, dict):
                for real_prop, fields in prop.items():
                    dicts.append(do_dict(real_prop, [obj.stringify(fields) for obj in self.fetch(real_prop)]))
            else:
                fetch_prop = self.fetch(prop)
                # noinspection PyComparisonWithNone
                if fetch_prop == None:
                    fetch_prop = None
                elif isinstance(fetch_prop, EmbeddedObject):
                    fetch_prop = str(fetch_prop)
                elif isinstance(fetch_prop, bytes):
                    fetch_prop = base64.b64encode(fetch_prop).decode()
                dicts.append(do_dict(prop, fetch_prop))
        return merge_dict(*dicts)

    def fetch(self, path):
        """
        Возвращает из источника данных значение по переданному пути path
        @param path: Путь для получения значение
        @return: Значение запрашиваемого значения
        """
        def get_level(last_known_source, path_list):
            if not last_known_source:
                return None
            name, path_list = path_list[0], path_list[1:]
            elem_num_found = re.search(".+\[(\d+)\]", name)
            if elem_num_found:
                elem_num = int(elem_num_found.group(1))
                name = name.replace("[%d]" % elem_num, "")
                value = last_known_source.__getattribute__(name)[elem_num]
            else:
                value = last_known_source.__getattribute__(name)
            return get_level(value, path_list) if len(path_list) else value

        return get_level(self, path.split(".")) if path else None

    def get_data_for_write_operation(self) -> dict:
        """
        Возвращает данные модели, которые могли бы быть использованы при вставке записи
        Эти данные должны уникально идентифицировать объект. Пустые (незаполненные) поля отбрасываются
        @return Словарь с данными для записи в БД
        @rtype : dict

        """
        return {
            key: val for key, val in self.get_data().items() if any([
                isinstance(val, RecordModel),
                self.mapper.is_list_value(val) and val.is_changed(),
                not self.mapper.is_none_value(val)
            ])
        }

    def values(self, filter_lambda=None):
        for property_name in self.mapper.get_properties():
            if not filter_lambda or (filter_lambda and filter_lambda(self.__dict__.get(property_name))):
                yield self.__dict__.get(property_name)

    def is_changed(self):
        """ Возвращает признак того, изменялась ли модель с момента загрузки из базы данных или нет """
        if not self.cant_calc_changed:
            with CalcChangesLock(self):
                return any(self.values(lambda v: isinstance(v, TrackChangesValue) and v.is_changed())) or self._changed

    def __setattr__(self, name, val):
        """ При любом изменении полей модели необходимо инициализировать модель """
        mapper = object.__getattribute__(self, "__dict__").get("mapper")
        if mapper and name in mapper.get_properties():
            self.exec_lazy_loading()
            self.mark_as_changed()
        object.__setattr__(self, name, val)

    def pretty_print(self, properties=None, bytes_len=10):
        """
        Pretty print collection items
        :param bounds:     Условия выборки записей
        :param params:     Параметры выборки (сортировка, лимит)
        :param properties  Поля которые надо отобразить. Если не заполнено то отобразятся все
        :param bytes_len   Максимальная длина поля для bytes
        """
        from prettytable import PrettyTable

        if not properties:
            properties = self.mapper.get_properties()
        properties = sorted(properties)

        # Наполнение таблицы
        table = PrettyTable()

        data = self.stringify(properties)

        # Имена колонок известны только теперь
        # т.к. .stringify схлопывает свойства подмодели в одно поле  по имени подмодели
        if not table.field_names:
            table.field_names = data.keys()

        for key, value in data.items():
            data[key] = value[:bytes_len] if isinstance(value, bytes) and bytes_len is not None else str(value)

        table.add_row(data.values())

        # Если в таблице нет данных то выводим пустую
        if not table.field_names:
            table.field_names = properties

        # Печать таблицы
        table.align = 'l'
        print(str(self.__class__) + '\n' + str(table))

    def __getattribute__(self, name):
        """ При любом обращении к полям модели необходимо инициализировать модель """
        mapper = object.__getattribute__(self, "__dict__").get("mapper")
        # Список полей первичного ключа
        if mapper and name in mapper.get_properties() and name not in self.primary.to_list():
            self.exec_lazy_loading()

        if name == "validate":
            return object.__getattribute__(self, "recursive_validate")

        return object.__getattribute__(self, name)

    def __eq__(self, other):
        """
        Переопределяем метод сравнения двух экземпляров класса
        @param other: Второй экземпляр класса
        @return: Результат сравнения

        """
        if isinstance(other, RecordModel):
            first = self.primary.get_value()
            second = other.primary.get_value()
            return first == second if first and second else self.get_data() == other.get_data()
        else:
            return False


class EmbeddedObject(ValueInside, metaclass=ABCMeta):
    """
    Класс для создания моделей, для которых в БД может храниться только одно значение,
    на основе которого должно происходить конструирование экземпляров класса этой модели
    """
    value = None
    value_type = str

    def get_value(self):
        """
        @return: Значение, которое будет храниться в поле таблицы БД
        и на основе которого будет конструироваться экземпляр данного класса
        """
        return self.value

    @classmethod
    def get_value_type(cls):
        """
        @return: Тип значения, которое будует храниться в БД в качестве идентификатора данной модели
        """
        return cls.value_type

    def __str__(self):
        return str(self.get_value())

    def __eq__(self, other):
        return isinstance(other, EmbeddedObject)\
            and self.get_value() == other.get_value()


class EmbeddedObjectFactory(object):
    """ Фабрика встраиваемых объектов """
    def __new__(cls, *args):
        assert len(args) <= 1
        return cls.get_instance(args[0]) if len(args) else object.__new__(cls)

    @classmethod
    def get_instance(cls, value):
        """
        @param value: Значение для конструирования экземпляра класса
        @return: Возвращает экземпляр класса, корректного для данного value
        """
        for obj in cls.all():
            if obj.get_value() == value:
                return obj

        # noinspection PyComparisonWithNone
        if value != None:
            raise EmbeddedObjectFactoryException('There are no factory for "%s"' % value)

    @classmethod
    def all(cls):
        return (obj() for obj in cls.__dict__.values() if type(obj) == ABCMeta and issubclass(obj, EmbeddedObject))


class TableModelCache(object):
    """ Класс для кэширования моделей уже проинициализированных моделей """
    def __init__(self, mapper, pool=None):
        self._mapper = mapper
        self._pool = pool
        self._cache = {}
        self._ids_cache = {}

    def get(self, model_type, primary_id):
        """
        Возвращает данные для модели типа model_type, первичный ключ которой равен primary_id
        :param model_type:   Тип модели
        :param primary_id:   Значение первичного ключа
        """
        primary_id = primary_id.get_value(deep=True) if isinstance(primary_id, ValueInside) else primary_id
        model_cache = self._cache.get(model_type)
        if model_cache:
            if type(model_cache) is not dict:
                self._cache[model_type] = self._cache[model_type](model_type)
                return self._cache[model_type].get(json.dumps(primary_id) if isinstance(primary_id, dict) else primary_id)
            return model_cache.get(json.dumps(primary_id) if isinstance(primary_id, dict) else primary_id)

    def cache(self, rows):
        """
        Выполняет кэширование для будущего использования
        :param rows:    Список строк для кэширования
        """
        # Сперва получим имена итересующих нас полей (Кэшируются только данные для полей Link и List)
        cache = {}
        field_names_for_cache = {}
        for field_name in self._mapper.get_properties():
            mf = self._mapper.get_property(field_name)
            if mf and self._mapper.is_rel(mf):
                field_names_for_cache[field_name] = {"mapper": mf.get_items_collection_mapper(), "mapper_field": mf}
                cache[mf.get_items_collection_mapper()] = []
        for row in rows:
            for field_name in field_names_for_cache.keys():
                if None != row.get(field_name):
                    if isinstance(row[field_name], ValueInside):
                        cache[field_names_for_cache[field_name]["mapper"]].append(row[field_name].get_value())
                    elif self._mapper.is_list(field_names_for_cache[field_name]["mapper_field"]):
                        for obj in row[field_name]:
                            cache[field_names_for_cache[field_name]["mapper"]].append(obj.primary.get_value())

        for mapper in cache:
            if len(cache[mapper]) > 0:
                self._ids_cache[mapper] = cache[mapper]
                self._cache[mapper] = self._get_mapper_cache

    def _get_mapper_cache(self, m):
        """ Собирает кэш маппера из внешней переменной cache """
        mapper_cache = {}
        if m.primary.compound:
            for item in m.get_new_collection(model_pool=self._pool).get_items({"or": self._ids_cache[m]}):
                key = item.primary.get_value(deep=True)
                mapper_cache[json.dumps(key)] = item.get_data()
        else:
            for item in m.get_new_collection(model_pool=self._pool).get_items({m.primary.name(): ("in", self._ids_cache[m])}):
                key = item.primary.get_value(deep=True)
                mapper_cache[key] = item.get_data()
        return mapper_cache


class Transaction(object):
    def __init__(self, pool):
        self.pool = pool

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("TX", exc_type)
        if self.pool.in_transaction:
            if exc_type:
                print("TXRB", exc_type)
                self.rollback()
            else:
                self.commit()

    def __del__(self):
        if self.pool.in_transaction:
            self.rollback()

    def start(self):
        self.pool.in_transaction = True
        self.pool.db.start_transaction()

    def commit(self):
        self.pool.db.commit()
        self.pool.in_transaction = False

    def rollback(self):
        self.pool.db.rollback()
        self.pool.in_transaction = False