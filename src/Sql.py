"""
Классы для работы с SQL
"""

from abc import abstractmethod, ABCMeta


class PlaceHoldersCounter(object):
    """ Объект для подсчета плейсхолдеров, использованных в запросе """

    def __init__(self):
        self.current = 0

    def get(self) -> int:
        """
        Возвращает новое значение для плейсхолдера
        @rtype : int
        @return: Новое значение для плейсхолдера

        """
        self.current += 1
        return self.current


class SqlBuilder(object, metaclass=ABCMeta):
    """ Класс реализующий логику построения SQL-запросов для всех SQL-совместимых баз данных """
    def __init__(self):
        self.placeholders_counter = 0

    # Определение соответствий операторов сравнения
    operators = {
        "e": " = ", "ne": " != ",
        "gt": " > ", "lt": " < ",
        "gte": " >= ", "lte": " <= ",
        "in": " IN ", "nin": " NOT IN ",
        "match": " LIKE "
    }

    def fields_enumeration(self, field_list: list, main_table: str=None, joins: list=None, conditions=None) -> str:
        """
        Возвращает строку с перечислением полей, оформленную в соответствии с синтаксисом SQL
        @param field_list: Список полей
        @type field_list: list
        @param main_table: Имя основной таблицы
        @type main_table: str
        @return : Строка с перечислением полей через запятую
        @rtype : str

        """
        return ", ".join([
            self.field(field, main_table, joins, conditions) if type(field) == str else "%s as %s" % (
                self.field(field[0], main_table, joins, conditions), self.wrap_alias(field[1])
            ) for field in field_list
        ])

    def fields_comparisons(self, data: dict, placeholders_counter: PlaceHoldersCounter, main_table: str=None) -> str:
        """
        Возвращает последовательность (строка, [данные]),
        где строка - это строка со сравнением полей (строка условий),
        а данные - это список значений, которые необходимо подставить
        в запрос на место плейсхолдеров (для параметризованных запросов)
        Принимает словарь с данными, имя основной таблицы (опционально)
        В словаре с даными, ключом может быть либо тип конкатенации условий ("and", "or"), либо имя поля таблицы
        В качестве значений в первом случае будут словари оформленные по этому же принципу,
        а во втором - либо строка со значением, либо последовательность (оператор сравнения, значение)
        @param data: Условия для обработки
        @type data: dict
        @param main_table: Основная таблица
        @type main_table: str
        @param placeholders_counter: Счетчик плейсхолдеров
        @type placeholders_counter: int
        @return : Данные сравнения полей с некими значениями (условия выборки)
        @rtype : str

        """
        conditions, values = [], []
        for key in data:
            # Ключ может быть модификатором объединения
            if key in ["and", "or"]:
                # Если это так, то для ответа понадобятся промежуточные результаты вычислений
                tconditions, tvalues = [], []
                # Значение - список словарей с условиями
                for conditionsDict in data[key]:
                    # Для каждого словаря получаем сгенерированные условия
                    res = self.fields_comparisons(conditionsDict, placeholders_counter, main_table)
                    tconditions.append(res[0])
                    tvalues += res[1]
                    # Добавляем их в общую коллекцию
                conditions.append("(%s)" % (" %s " % (key.upper())).join(tconditions))
                values += tvalues
            else:
                # В другом случае, ключ является именем поля, а значение может быть последовательностью или строкой
                field = key
                value = data[key]
                # Если значение - строка, то используем оператор сравнения
                if type(value) not in [tuple, set, list]:
                    conditions.append(
                        "%s = %s" % (
                            self.field(field, main_table), self.placeholder_controller(value, placeholders_counter)
                        )
                    )
                    values.append(value)
                # В последовательности, первый параметр это оператор сравнения, второй - значение
                else:
                    operator, value = value
                    # В зависимости от типа оператора мы можем обработать значение
                    value = self.value(value, operator)
                    # Оператор проверки на существование значения выбивается из общего списка,
                    # так как не обладает значением
                    if operator == "exists":
                        conditions.append(
                            "%s%s" % (self.field(field, main_table), " IS %sNULL" % ("NOT " if value is True else ""))
                        )
                    else:
                        conditions.append(
                            "%s%s%s" % (
                                self.field(field, main_table),
                                SqlBuilder.operators[operator],
                                self.placeholder_controller(value, placeholders_counter)
                            )
                        )
                        # Если у нас список значений, то добавляем их все
                        if type(value) == list:
                            for v in value:
                                values.append(v)
                        else:
                            values.append(value)
        return "(%s)" % " AND ".join(conditions), values

    def field(self, field: str, table: str=None, joins: list=None, conditions=None) -> str:
        """
        Обрабатывает переданное имя поля и таблицы в соответствии с синтаксисом SQL
        @param field:
        @type field: str
        @param table:
        @type table: str
        @return : Обработанное обращение к полю таблицы
        @rtype : str

        """

        if field.find(".") > -1:
            path = field.split(".")
            field = path.pop()
            table = "_".join(path)
        if field.find("+") > -1:
            return "%s as %s" % (
                self.aggregate_function(self.concat_ws_function(field, table, "$!"), table, joins, conditions),
                self.wrap_alias(field)
            )
        if field.endswith("]"):
            field = field.split("[")
            alias = "%s_%s" % (field[1].replace("]", ""), field[0])
            return "%s as %s" % (
                self.aggregate_function(self.field(field[0], table), table, joins, conditions),
                self.wrap_alias(alias)
            )
        return "%s.%s" % (self.wrap_table(table), self.wrap_field(field)) if table else "%s" % self.wrap_field(field)

    def placeholder_controller(self, value, placeholders_counter: PlaceHoldersCounter) -> str:
        """
        Возвращает корректный placeholder в зависимости от переданного значения
        Контролирует логику подсчета позиций и запрашивает правильный тип у субкласса
        @param value: Значение, для которого требуется получить плейсхолдер
        @type value: *
        @param placeholders_counter: Экземпляр счетчика плейсхолдеров, используемый в формируемом запросе
        @type placeholders_counter: PlaceHoldersCounter
        @return : Плейсхолдер в корректном формате
        @rtype : str

        """
        get_ph = lambda v: self.placeholder(v, placeholders_counter.get())
        get_ph_list = lambda vs: ", ".join([get_ph(v) for v in vs])
        # Обычный плейсхолдер
        if type(value) not in [tuple, set, list]:
            return get_ph(value)
        # Перечисление групп плейсхолдеров черерз запятую (для множественного инсерта, например)
        elif type(value) == list and type(value[0]) == list:
            return get_ph_list(value)
        # n-ое количество ph в скобках для списков значений
        else:
            return "(%s)" % get_ph_list(value)

    @staticmethod
    def value(value, operator: str) -> str:
        """
        Переопределяет значение в зависимости от оператора сравнения или возвращает без изменений
        @param value: Исходное значение
        @type value: *
        @param operator: Оператор сравнения
        @type operator: str
        @return : Переопределенное значение
        @rtype : str

        """
        return value.replace("*", "%") if operator == "match" else value

    def wrap_ordering_cmd(self, order_cmd: tuple, table: str) -> str:
        """
        Обрабатывет единичную директиву сортировки: tuple("имя поля", "НАПРАВЛЕНИЕ")
        @param order_cmd: tuple("имя поля", "НАПРАВЛЕНИЕ")
        @type order_cmd: tuple
        @param table: Имя основной таблицы
        @type table: str
        @return: "имя поля" "НАПРАВЛЕНИЕ"
        @rtype : str

        """
        fname, ord_direction = order_cmd
        if fname.find(".") > -1:
            table, fname = fname.split(".")
        return "%s.%s %s" % (self.wrap_table(table), self.wrap_field(fname), ord_direction.upper())

    @staticmethod
    def limit_section(limit: int) -> str:
        """
        Возвращает секцию запроса ограничаювающую кол-во записей в результате
        @param limit: Ограничение выборки
        @type limit: int
        @return: Секция sql-запроса
        @rtype : str

        """
        return "LIMIT %d" % limit if limit else ""

    def order_section(self, order_data: tuple or list, main_table: str) -> str:
        """
        Возвращает секцию запроса, отвечющую за сортировку записей в результате выборки
        @param order_data: Параметры сортировки
        @type order_data: tuple or list
        @param main_table: Имя основной таблицы выборки
        @type main_table: str
        @return: Секция сортировки результатов sql-запроса
        @rtype : str

        """
        if order_data:
            if type(order_data) is tuple:
                order_data = [order_data]
            return "ORDER BY %s" % (", ".join(
                [self.wrap_ordering_cmd(ord_option, main_table) for ord_option in order_data]
            ))

    @property
    def group_method(self):
        """ Метод группировки полей при использовании GROUP_CONCAT аггрегации """
        return "all_keys"

    @abstractmethod
    def wrap_table(self, table: str) -> str:
        """
        Обрамляет кавычками имена таблиц
        @param table: Имя таблицы
        @type table: str
        @return : Обрамленное кавычками имя таблицы
        @rtype : str

        """

    @abstractmethod
    def wrap_field(self, field) -> str:
        """
        Обрамляет кавычками имена полей таблиц
        @type field: str
        @param field: Имя поля
        @return : Обрамленное кавычками имя поля таблицы
        @rtype : str

        """

    @abstractmethod
    def wrap_alias(self, alias: str) -> str:
        """
        Обрамляет кавычками алиасы полей
        @param alias:
        @type alias: str
        @return : Обрамленный кавычками алиас для поля таблицы или имени таблицы
        @rtype : str

        """

    @abstractmethod
    def aggregate_function(self, field: str, table: str, joins: list=None, conditions=None) -> str:
        """
        Возвращает имя аггрегатной функции для группирования строк
        @param field: Имя поля для объединения значений в массив
        @type field: str
        @return: имя аггрегатной функции
        @rtype : str

        """

    @abstractmethod
    def concat_ws_function(self, field: str, table: str, separator: str) -> str:
        """
        Возвращает функцию конкатенации полей через разделитель
        @param field: Имя поля для объединения
        @type field: str
        @param table: Имя таблицы, которой принадлежит поле
        @type table: str
        @param separator: Разделитель
        @type separator: str
        @rtype : str

        """

    @abstractmethod
    def placeholder(self, value, counter: int) -> str:
        """
        Возвращает корректный placeholder в зависимости от переданного значения
        @param value: Значение, для которго требуется сгенерировать плейсхолдер
        @type value: *
        @param counter: Счетчик плейсхолдеров
        @type counter: PlaceHoldersCounter
        @return : Плейсхолдер для текущей позиции в соответствии с переданным значением и состоянием счетчика
        @rtype : str

        """

    @abstractmethod
    def limit_skip_section(self, limit_value: int, skip_value: int) -> str:
        """
        Возвращает секцию запроса, отвечающую за пропуск записей в результате выборки и ограничение выборки
        @param limit_value: Количество записей, которое необходимо вернуть
        @type limit_value: int
        @param skip_value: Количество записей, которое требуется пропустить
        @type skip_value: int
        @return: Секция пропуска записей результата sql-запроса и ограничения выборки
        @rtype : str

        """

    @staticmethod
    @abstractmethod
    def build_select_query(query) -> str:
        """
        Строит sql-запрос на выборку строк из таблицы на основе имеющейся информации
        @param query: Экземпляр запроса на выборку строк
        @type query: SelectQuery
        @return : sql-запрос
        @rtype : str

        """

    @staticmethod
    @abstractmethod
    def build_delete_query(query) -> str:
        """
        Строит запрос на удаление строк из таблицы
        @param query: Экземпляр запроса на удаление строк
        @type query: DeleteQuery
        @return : sql-запрос
        @rtype : str

        """

    @abstractmethod
    def build_insert_query(self, query) -> str:
        """
        Строит запрос на вставку данных в таблицу
        @param query: Экземпляр запроса на вставку данных
        @type query: InsertQuery
        @return: sql-запрос
        @rtype : str

        """

    @abstractmethod
    def build_update_query(self, query) -> str:
        """
        Строит запрос на обновление данных в таблице по условиям
        @param query: Экземпляр запроса на обновление данных
        @type query: UpdateQuery
        @return: sql-запрос
        @rtype : str

        """


class BaseSqlQuery(object):
    """ Базовый класс для построения SQL-запросов """

    def __init__(self, builder: SqlBuilder):
        """
        Конструктор
        @type builder: SqlBuilder
        @param builder: Экземпляр класса для построения SQL-запросов

        """
        self.builder = builder
        self.table_name = ""
        self.primary = None
        self.data = []
        self.placeholders_counter = PlaceHoldersCounter()

    def set_table_name(self, table_name: str):
        """
        Устанавливает имя таблицы, над которой производится операция
        @param table_name: Имя таблицы
        @type table_name: str

        """
        self.table_name = table_name

    def get_table_name(self) -> str:
        """
        Возвращает имя таблицы, обернутое кавычками
        @return Имя таблицы внутри кавычек
        @rtype : str

        """
        return self.builder.wrap_table(self.table_name)

    def set_primary(self, primary: str):
        """
        Устанавливает имя первичного ключа таблицы
        @param primary: Название поля первичного ключа
        @type primary: str

        """
        self.primary = primary

    def get_primary(self) -> str:
        """
        Возвращает имя первичного ключа текущей таблицы
        @return Имя первичного ключа
        @rtype : str

        """
        return self.primary

    def get_query_data(self) -> list:
        """
        Возвращает данные для запроса
        @return Список параметров для запроса
        @rtype : list

        """
        return self.data


class JoinMixin(BaseSqlQuery):
    """ Миксин добавляющий методы для работы с объединением таблиц """

    def __init__(self, builder: SqlBuilder):
        """
        Конструктор
        @type builder: SqlBuilder
        @param builder: Экземпляр класса для построения SQL-запросов

        """
        super().__init__(builder)
        self.joins = []

    def set_joins(self, joins: list):
        """
        @param joins: Список джойнов внешних таблиц
        @type joins: list

        """
        self.joins = joins

    def get_join_section(self) -> str:
        """
        Возвращает секцию JOIN для запроса
        @return: Секция JOIN для запроса
        @rtype : str

        """
        return " ".join(list([j.stringify(self.builder) for j in self.joins]))

    def get_joined_tables(self) -> list:
        """
        Возвращает секцию USING для запроса
        @return: Секция USING для запроса
        @rtype : list

        """
        return [
            "%s as %s" % (
                self.builder.wrap_table(j.foreign_table_name), self.builder.wrap_table(j.alias)
            ) for j in self.joins
        ]

    def get_join_conditions(self) -> str:
        """
        Возвращает условия связывания таблиц
        @return: Условия связывания таблиц, объединенные через AND
        @rtype : str

        """
        return " AND ".join([j.stringify_condition(self.builder) for j in self.joins])


class ConditionsMixin(BaseSqlQuery):
    """ Миксин добавляющий методы для работы с условиями выборки """

    def __init__(self, builder: SqlBuilder):
        """
        Конструктор
        @type builder: SqlBuilder
        @param builder: Экземпляр класса для построения SQL-запросов

        """
        super().__init__(builder)
        self.conditions = {}
        self.params = {
            "skip": 0, "order": None
        }

    def set_conditions(self, conditions: dict):
        """
        Устанавливает условия выборки
        @param conditions: Условия выборки или применения запроса
        @type conditions: dict

        """
        self.conditions = conditions

    def get_conditions(self) -> str:
        """
        Возвращает секцию условий выборки
        @rtype : str

        """
        if self.conditions:
            condition_string, conditions_data = self.builder.fields_comparisons(
                self.conditions, self.placeholders_counter, self.table_name
            )
            self.data += conditions_data
            return condition_string

    def set_params(self, params: dict):
        """
        Устанавливает параметры выборки
        @type params: dict
        @param params: Словарь с параметрами выборки

        """
        if params:
            for p in params:
                self.params[p] = params[p]

    def get_params_section(self) -> str:
        """
        Возвращает секцию параметров выборки (sort, limit, offset)
        @return : Секция запроса с параметрами выборки
        @rtype : str

        """
        order_section = self.builder.order_section(self.params.get("order"), self.table_name)
        limit_skip_section = self.builder.limit_skip_section(self.params.get("limit"), self.params.get("skip"))
        return "%s %s" % (
            order_section if self.params.get("order") else "",
            limit_skip_section
        )

    def get_limit_section(self) -> str:
        return self.builder.limit_skip_section(self.params.get("limit"), self.params.get("skip"))

    def get_limit_only_section(self) -> str:
        return self.builder.limit_section(self.params.get("limit"))

    def get_order_section(self) -> str:
        """
        """
        return self.builder.order_section(self.params.get("order"), self.table_name)

    @staticmethod
    def concat_conditions(conditions_list: list) -> str:
        """
        Конкатенирует не пустые условия выборки

        @param conditions_list:     Список условий выборки
        @type conditions_list: list

        @return: Объединенные условия выборки или применения запроса
        @rtype : str

        """
        return " AND ".join(list(filter(None, conditions_list)))


class SelectQuery(ConditionsMixin, JoinMixin, BaseSqlQuery):
    """ SQL-Запрос на выборку данных из таблицы базы данных """

    def __init__(self, builder: SqlBuilder):
        """
        Конструктор
        @type builder: SqlBuilder
        @param builder: Экземпляр класса для построения SQL-запросов

        """
        super().__init__(builder)
        self.fields = []

    def set_fields(self, fields: list):
        """
        Устанавливает список требуемых для выборки полей
        @type fields: list
        @param fields: Список полей для выборки

        """
        self.fields = fields

    def get_fields_section(self) -> str:
        """
        Возвращает перечисления запрошенных полей через запятую для использования в запросе
        @return : Перечисление запрошенных полей через запятую
        @rtype : str

        """
        return self.builder.fields_enumeration(self.fields, self.table_name, self.joins, lambda: self.get_conditions())

    def get_groupby_section(self) -> str:
        """
        Возвращает секцию группировки записей по полям
        @return: Секция группировки записей
        @rtype : str

        """
        need_to_group = len(list(filter(lambda f: f.endswith("]"), self.fields))) > 0
        if not need_to_group:
            return ""

        if self.builder.group_method == "primary_key" and self.primary is not None:
            return self.builder.wrap_field(self.primary if self.primary else "")

        fields = list(filter(lambda f: f.endswith("]") is False, self.fields))
        fields_from_order = self.params.get("order") or []
        if type(fields_from_order) is tuple:
            fields_from_order = [fields_from_order]

        for field in fields_from_order:
            if field[0] not in fields:
                fields.append(field[0])
        return self.builder.fields_enumeration(fields, self.table_name) if len(fields) > 0 else ""

    def build(self) -> str:
        """
        Строит запрос с использованием текущего билдера и имеющихся данных о запросе
        @return: Sql-запрос
        @rtype : str

        """
        return self.builder.build_select_query(self)


class CountQuery(SelectQuery):
    """ Класс запросов на подсчет строк в таблице в соответствии с переданными условиями """
    def get_fields_section(self) -> str:
        """ Возвращает директиву на возврат подсчета строк вместо перечисления полей
        @return: Директива count(*)
        @rtype : str
        """
        return "COUNT(*)"

    def get_params_section(self) -> str:
        """
        Возвращает секцию параметров выборки (sort, limit, offset)
        @return : Секция запроса с параметрами выборки
        @rtype : str

        """
        return ""


class DeleteQuery(ConditionsMixin, JoinMixin, BaseSqlQuery):
    """ Класс sql-запросов на удаление записей из таблицы """
    def build(self) -> str:
        """
        Строит запрос с использованием текущего билдера и имеющихся данных о запросе
        @return: Sql-запрос
        @rtype : str

        """
        return self.builder.build_delete_query(self)


class InsertQuery(BaseSqlQuery):
    """ Класс sql-запросов на добавление записей в таблицу """

    def __init__(self, builder: SqlBuilder):
        """
        Конструктор
        @type builder: SqlBuilder
        @param builder: Экземпляр класса для построения SQL-запросов

        """
        super().__init__(builder)
        self.affected_fields = ""
        self.insert_placeholders = []

    def parse_data(self, data: dict) -> tuple:
        """
        Разбирает переданный словарь на части (поля, плейсхолдеры, данные)
        @param data: Словарь с данными для вставки
        @type data: dict
        @return: Последовательность '"One", "Two", "Three"', [?, ?, ?], [1, 2, 3]
        @rtype : tuple

        """
        values = list(data.values())
        fields = self.builder.fields_enumeration(list(data.keys()))
        placeholders = [self.builder.placeholder_controller(values, self.placeholders_counter)]
        return fields, placeholders, values

    def set_insert_data(self, data: list or dict):
        """
        Разбирает на части и сохраняет данные для вставки в таблицы
        @type data: list of dict or dict
        @param data: Данные для вставки в виде словаря или списка словарей

        """
        if type(data) is list:
            for row in data:
                af, ip, d = self.parse_data(row)
                self.affected_fields = af
                self.insert_placeholders += ip
                self.data += d
        else:
            self.affected_fields, self.insert_placeholders, self.data = self.parse_data(data)

    def get_fields(self) -> str:
        """
        Возвращает секцию перечисления полей, в которые происходит вставка данных
        @return: Часть запроса на вставку данных с перечислением имен полей
        @rtype : str

        """
        return "(%s)" % self.affected_fields

    def get_placeholders(self) -> str:
        """
        Возвращает секцию плейсхолдеров для вставляемых значений
        @rtype : str
        @return: Часть запроса на вставку данных с перечислением плесхолдеров

        """
        return ", ".join(self.insert_placeholders)

    def build(self) -> str:
        """
        Строит запрос с использованием текущего билдера и имеющихся данных о запросе
        @return: Sql-запрос
        @rtype : str

        """
        return self.builder.build_insert_query(self)


class UpdateQuery(ConditionsMixin, JoinMixin, BaseSqlQuery):
    """ Класс sql-запросов на изменение записей в таблице """

    def __init__(self, builder: SqlBuilder):
        """
        Конструктор
        @type builder: SqlBuilder
        @param builder: Экземпляр класса для построения SQL-запросов

        """
        super().__init__(builder)
        self.data_for_update = {}
        self.values_assignment = ""
        self._use_table_name_in_set_section = False

    def use_table_name_in_set_section(self):
        """ Заставляет использоваться имя таблицы при указании имени поля, которое требуется обновить """
        self._use_table_name_in_set_section = True

    def set_update_data(self, data: dict):
        """
        Сохраняет данные для обновления записей
        @param data: Новые значения для указанных полей в формате словаря
        @type data: dict

        """
        self.data_for_update = data

    def get_values_assignment(self) -> str:
        """
        Возвращает секцию запроса, где идет назначение полям их новых значений (через плейсхолдеры)
        @return: секция запроса с назначением новых значений
        @rtype : str

        """
        values_assignment, values = self.builder.fields_comparisons(
            self.data_for_update, self.placeholders_counter,
            self.table_name if self._use_table_name_in_set_section else None
        )
        values_assignment = values_assignment.replace("(", "").replace(")", "").replace(" AND ", ", ")
        self.data += values
        return values_assignment

    def build(self) -> str:
        """
        Строит запрос с использованием текущего билдера и имеющихся данных о запросе
        @return: Sql-запрос
        @rtype : str

        """
        return self.builder.build_update_query(self)


class PgDbField(object):
    """ Класс реального поля таблицы базы данных PostgreSQL"""

    def __init__(self, name, default, can_be_null, db_type, max_length, is_primary=False):
        self.name = name
        self.is_primary = is_primary
        self.default = default
        self.can_be_null = can_be_null
        self.db_type = db_type
        self.max_length = max_length
        self.autoincremented = False


class MySqlDbField(object):
    """ Класс реального поля таблицы базы данных MySQL"""

    def __init__(self, name, db_type, can_be_null, key, default, extra):
        self.name = name
        self.is_primary = key == "PRI"
        self.default = default
        self.can_be_null = can_be_null == "YES"
        self.db_type = db_type.split("(")[0]
        self.max_length = db_type.split("(")[1].strip(")") if db_type.find("(") > -1 else None
        self.extra = extra
        self.autoincremented = extra == "auto_increment"


class MsSqlDbField(object):
    """ Класс реального поля таблицы базы данных MsSQL"""

    def __init__(self, name, can_be_null, db_type, max_length, default, key, identity):
        self.name = name
        self.is_primary = key == name
        self.default = default
        self.can_be_null = can_be_null == "YES"
        self.db_type = db_type.split("(")[0]
        self.max_length = db_type.split("(")[1].strip(")") if db_type.find("(") > -1 else max_length
        self.autoincremented = bool(identity)


class QueriesAnalyzer(object):
    """ Класс для хранения отладочной информации о выполняемых запросах """

    def __init__(self):
        self.query_counter = 0
        self.queries = []

    def log(self, query: str, params: list):
        """
        Выполняет логирование запроса
        @param query:   Текст запроса
        @type query: str
        @param params:  Параметры для запроса
        @type params: list

        """
        self.queries.append((query, params))
        self.query_counter += 1

    def count(self) -> int:
        """
        Возвращает количество выполненных с момента начала отслеживания запросов
        @return:
        @rtype : int

        """
        return self.query_counter

    def show(self) -> list:
        """
        Возвращает список выполненных с момента начала отслеживания запросов
        @return:
        @rtype : list

        """
        return self.queries


class AdapterLogger(object):

    def __init__(self):
        self.query_analyzer = None

    def start_logging(self):
        """ Начинает отслеживать выполняемый адаптером запросы """
        self.query_analyzer = QueriesAnalyzer()

    def stop_logging(self):
        """ Прекращает отслеживание выполняемых запросов """
        self.query_analyzer = None

    def count_queries(self) -> int:
        """
        Возвращает количество выполненных за сеанс отлсеживания запросов
        @return:
        @rtype : int

        """
        return self.query_analyzer.count() if self.query_analyzer else 0

    def show_queries(self) -> list:
        """
        Возвращает список выполненных за сеанс отлсеживания запросов
        @return:
        @rtype : list

        """
        return self.query_analyzer.show() if self.query_analyzer else []


class Adapter(AdapterLogger, metaclass=ABCMeta):
    """ Базовый класс для создания адаптеров к СУБД """

    def __init__(self):
        self.connection_data = (None,)
        self.connection = None
        self.autocommit = True
        self.tx = None
        self.dublicate_record_exception = None
        self.query_builder = self.get_query_builder()
        super().__init__()

    def connect(self, connection_data: tuple, autocommit=True):
        """
        Выполняет подключение к СУБД
        @param connection_data: Последовательность данных для подключения к СУБД
        @type connection_data: tuple
        """
        self.connection_data = connection_data
        self.autocommit = autocommit
        self.connection = self.open_connection(self.connection_data, self.autocommit)
        return self if self.connection else False

    def reconnect(self):
        """ Выполняет переподключение к серверу базы данных """
        self.close()
        self.connect(self.connection_data, self.autocommit)

    def close(self):
        """ Закрывает соединение с базой данных """
        if self.connection:
            self.close_connection()

    def execute(self, sql: str, params: list=None):
        """
        Выполняет запрос и сразу возвращает первое значение из генератора

        @param sql: Sql-запрос
        @type sql: str

        @param params: Параметры для запроса
        @type params: list

        @return : Результат выполнения запроса
        @rtype : *

        """
        res = list(self.generate(sql, params))
        return res[0][0] if len(res) > 0 else None

    def generate(self, sql: str, params: list=None):
        """
        Выполняет запрос и сохраняет статистическую, отладочную информацию

        @param sql: Sql-запрос
        @type sql: str

        @param params: Параметры для запроса
        @type params: list

        @return : Результат выполнения запроса
        @rtype : generatorType
        """
        if self.query_analyzer:
            self.query_analyzer.log(sql, params)
        for row in self.execute_query(sql, params):
            yield row

    def get_value(self, sql, params=None):
        """
        Возвращает значение одного из полей одной из строк результата выполнения запроса
        :param sql:         SQL-Запрос
        :param params:      Параметры для запроса
        :return: str:       Результат выполнения
        """
        result = self.get_row(sql, params)
        if result and type(result) not in [str, int]:
            return result[0]
        else:
            return result

    def get_row(self, sql, params=None):
        """
        Возвращает одну записи таблицы
        :param sql:         SQL-Запрос
        :param params:      Параметры для запроса
        :return: list:      Результат выполнения
        """
        result = self.generate(sql, params)
        for line in result:
            return line
        return None

    def get_column(self, sql, params=None):
        """
        Возвращает генератор списка значений одной колонки таблицы для всех строк, удовлетворяющих условию выборки
        :param sql:         SQL-Запрос
        :param params:      Параметры для запроса
        :return: list:      Результат выполнения
        """
        for line in self.generate(sql, params):
            if type(line) not in [str, int]:
                yield line[0]
            else:
                yield line

    def get_rows(self, sql, params=None):
        """
        Возвращает генератор списка строк, удовлетворяющих условию выборки
        :param sql:                 SQL-Запрос
        :param params:              Параметры для запроса
        :return: GeneratorType      Результат выполнения
        """
        for row in self.generate(sql, params):
            yield row

    def count_query(self, table_name, conditions, joins=None):
        """
        Выполняет запрос на подсчет строк в таблице по заданным условиям
        :param table_name:      Имя таблицы
        :param conditions:      Условия подсчета строк
        :param joins:           Список джойнов, необходимых для выполнения запроса
        :return:
        """
        query = CountQuery(self.query_builder)
        query.set_table_name(table_name)
        query.set_conditions(conditions)
        query.set_joins(joins)
        return int(self.get_value(*query.build()))

    def insert_query(self, table_name, data, primary_key):
        """
        Выполняет запрос на вставку данных в таблицу
        :param table_name:      Имя таблицы
        :param data:            Данные для вставки
        :param primary_key:     Первичный ключ таблицы
        :return:                Значение первичного ключа добавленной записи
        """
        if data == list():
            return 0

        query = InsertQuery(self.query_builder)
        query.set_table_name(table_name)
        query.set_primary(primary_key.db_name() if primary_key.exists() else None)
        query.set_insert_data(data)
        res = self.get_value(*query.build())
        return res if primary_key and res not in ["DELETE", "INSERT", "UPDATE"] else 0

    def update_query(self, table_name, data, conditions, params=None, joins=None, primary_key=None):
        """
        Выполняет запрос на обновление данных в таблице в соответствии с условиями
        :param table_name:      Имя таблицы
        :param data:            Данные для обновления
        :param conditions:      Условия выборки
        :param params:          Параметры выборки
        :param joins:           Список join'ов
        :param primary_key:     Первичный ключ таблицы
        :return:
        """
        query = UpdateQuery(self.query_builder)
        query.set_table_name(table_name)
        query.set_update_data(data)
        query.set_conditions(conditions)
        query.set_params(params)
        query.set_joins(joins)
        query.set_primary(primary_key.db_name() if primary_key.exists() else None)
        res = self.get_column(*query.build())
        res = [item if item not in ["DELETE", "INSERT", "UPDATE"] else None for item in res]
        return res if None not in res else []

    def delete_query(self, table_name, conditions, joins=None):
        """
        Выполняет запрос на удаление строк из таблицы в соответствии с уловиями
        :param table_name:      Имя таблицы
        :param conditions:      Условия удаления
        :param joins:           Список джойнов
        :return:
        """
        query = DeleteQuery(self.query_builder)
        query.set_table_name(table_name)
        query.set_conditions(conditions)
        query.set_joins(joins if joins else [])
        return self.get_value(*query.build())

    def select_query(self, table_name, fields, conditions, params=None, joins=None, adapter_method=None, primary_key=None):
        """
        Выполняет запрос на получение строк из таблицы помощью указанного метода адаптера
        :param table_name:      Имя таблицы
        :param fields:          Список полей
        :param conditions:      Условия выборки
        :param params:          Параметры выборки
        :param joins:           Список джойнов
        :param adapter_method:  Метод адаптера, для выполнения запроса
        :return:
        """
        query = SelectQuery(self.query_builder)
        query.set_table_name(table_name)
        query.set_fields(fields)
        query.set_joins(joins)
        query.set_primary(primary_key.db_name() if primary_key.exists() else None)
        query.set_conditions(conditions)
        query.set_params(params)
        res = query.build()
        return self.__getattribute__(adapter_method)(*res)

    @abstractmethod
    def open_connection(self, connection_data, autocommit=True):
        """
        Открывает соединение с базой данных и возвращает его
        :param connection_data:  Данные для подключение к СУБД
        """
        pass

    @abstractmethod
    def close_connection(self):
        """ Закрывает текущее подключение """
        pass

    @abstractmethod
    def execute_query(self, sql, params) -> list:
        """
        Выполняет запрос средствами используемого коннектора
        :param sql: Sql-запрос
        :param params: Данные для запроса
        """

    @abstractmethod
    def execute_raw(self, sql):
        """
        Выполняет sql-сценарий. Неиспользует prepared statements
        @param sql: Текст sql-сценария
        @return: Результат выполнения
        """

    @abstractmethod
    def get_query_builder(self) -> SqlBuilder:
        """ Устанавливает объект для построения синтаксических конструкций специфичных для конкретной СУБД """

    @abstractmethod
    def get_table_fields(self, table_name):
        """
        Возвращает информацию  о полях таблицы базы данных
        :param table_name:  Имя таблицы
        :return: :raise:    AdapterException
        """

    @abstractmethod
    def start_transaction(self):
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def rollback(self):
        pass
