"""
Построители sql-синтаксиса для различных СУБД
"""

from .Sql import SqlBuilder, SelectQuery, DeleteQuery, InsertQuery, UpdateQuery


# noinspection PyMethodMayBeStatic
class PgSqlBuilder(SqlBuilder):
    """ Класс, наследующий SqlBuilder и реализующий специфичные для PgSQL особенности синтаксиса SQL """

    def wrap_table(self, table):
        """ Обрамляет кавычками имена таблиц
        :param table:   Имя таблицы
        """
        return '''"%s"''' % table

    def wrap_field(self, field):
        """ Обрамляет кавычками имена полей таблиц
        :param field:   Имя поля
        """
        return '''"%s"''' % field

    def wrap_alias(self, alias):
        """ Обрамляет кавычками алиасы полей
        :param alias:   Имя алиаса
        """
        return '''"%s"''' % alias

    def aggregate_function(self, field: str, table: str, joins: list=None, conditions: dict=None) -> str:
        """
        Возвращает имя аггрегатной функции для группирования строк
        @param field: Имя поля для группировки значений в массив
        @type field: str
        @return: имя аггрегатной функции
        @rtype : str

        """
        return "ARRAY_AGG(%s)" % field

    def concat_ws_function(self, field: str, table: str, separator: str):
        """
        Возвращает функцию конкатенации полей через разделитель
        @param field: Имя поля для объединения
        @type field: str
        @param table: Имя таблицы, которой принадлежит поле
        @type table: str
        @param separator: Разделитель
        @type separator: str

        """
        return "concat_ws('%s', %s)" % (
            separator,
            ", ".join([self.field(f.split("[")[0], f.split("[")[1].strip("]")) for f in field.split("+")])
        )

    # noinspection PyUnusedLocal
    def placeholder(self, value, counter):
        """Возвращает корректный placeholder в зависимости от переданного значения
        :param value:   Значение
        :param counter: Счетчик
        """
        return "$%s" % counter

    def limit_skip_section(self, limit_value: int, skip_value: int) -> str:
        """
        Возвращает секцию запроса, отвечающую за ограничение выборки и пропуск записей в результате выборки

        @param limit_value: Ограничение выборки
        @type limit_value: int

        @param skip_value: Количество записей, которое требуется пропустить
        @type skip_value: int

        @return: Секция пропуска записей и ограничение количества возвращаемых записей результата sql-запроса
        @rtype : str

        """
        return "%s OFFSET %d" % (self.limit_section(limit_value), skip_value)

    @staticmethod
    def build_select_query(query: SelectQuery) -> str:
        """
        Строит sql-запрос на выборку строк из таблицы на основе имеющейся информации
        @param query: Экземпляр запроса на выборку строк
        @type query: SelectQuery
        @return : sql-запрос
        @rtype : str

        """
        conditions = query.get_conditions()
        groupby = query.get_groupby_section()
        return '''SELECT %s FROM %s %s %s  %s %s ''' % (
            query.get_fields_section(), query.get_table_name(),
            query.get_join_section(),
            "WHERE %s" % conditions if conditions else "",
            "GROUP BY %s" % groupby if len(groupby) > 0 else "",
            query.get_params_section()
        ), query.get_query_data()

    @staticmethod
    def build_delete_query(query: DeleteQuery) -> str:
        """
        Строит запрос на удаление строк из таблицы
        @param query: Экземпляр запроса на удаление строк
        @type query: DeleteQuery
        @return : sql-запрос
        @rtype : str

        """
        conditions = query.concat_conditions([query.get_conditions(), query.get_join_conditions()])
        using = ", ".join(query.get_joined_tables())
        return '''DELETE FROM %s %s %s''' % (
            query.get_table_name(),
            "USING %s" % using if len(using) > 0 else "",
            "WHERE %s" % conditions if len(conditions) > 0 else ""
        ), query.get_query_data()

    def build_insert_query(self, query: InsertQuery) -> str:
        """
        Строит запрос на вставку данных в таблицу
        @param query: Экземпляр запроса на вставку данных
        @type query: InsertQuery
        @return: sql-запрос
        @rtype : str
        """
        return '''INSERT INTO %s %s VALUES %s %s ''' % (
            query.get_table_name(),
            query.get_fields(),
            query.get_placeholders(),
            "RETURNING %s.%s" % (
                query.get_table_name(), self.wrap_field(query.get_primary())
            ) if query.get_primary() else ""
        ), query.get_query_data()

    def build_update_query(self, query: UpdateQuery) -> str:
        """
        Строит запрос на обновление данных в таблице по условиям
        @param query: Экземпляр запроса на обновление данных
        @type query: UpdateQuery
        @return: sql-запрос
        @rtype : str

        """
        assignment = query.get_values_assignment()
        conditions = query.concat_conditions([query.get_conditions(), query.get_join_conditions()])
        from_tables = ", ".join(query.get_joined_tables())
        return '''UPDATE %s * SET %s %s %s %s %s %s ''' % (
            query.get_table_name(),
            assignment,
            "FROM %s" % from_tables if len(from_tables) > 0 else "",
            "WHERE %s" % conditions if len(conditions) > 0 else "",
            query.get_order_section() if query.get_order_section() else "",
            query.get_limit_only_section(),
            "RETURNING %s.%s" % (
                query.get_table_name(), self.wrap_field(query.get_primary())
            ) if query.get_primary() else ""
        ), query.get_query_data()


# noinspection PyMethodMayBeStatic
class MySqlBuilder(SqlBuilder):
    """ Класс, наследующий SqlBuilder и реализующий специфичные для MySQL особенности синтаксиса SQL """

    def wrap_table(self, table):
        """ Обрамляет кавычками имена таблиц
        :param table:   Имя таблицы
        """
        return '''`%s`''' % table

    def wrap_field(self, field):
        """ Обрамляет кавычками имена полей таблиц
        :param field:   Имя поля
        """
        return '''`%s`''' % field

    def wrap_alias(self, alias):
        """ Обрамляет кавычками алиасы полей
        :param alias:   Имя алиаса
        """
        return '''`%s`''' % alias

    def aggregate_function(self, field: str, table: str, joins: list=None, conditions: dict=None) -> str:
        """
        Возвращает имя аггрегатной функции для группирования строк
        @param field: Имя поля для группировки значений в массив
        @type field: str
        @return: имя аггрегатной функции
        @rtype : str

        """
        return "CONVERT(GROUP_CONCAT(DISTINCT(%s)) USING utf8)" % field

    @property
    def group_method(self):
        """ Метод группировки полей при использовании GROUP_CONCAT аггрегации """
        return "primary_key"

    def concat_ws_function(self, field: str, table: str, separator: str):
        """
        Возвращает функцию конкатенации полей через разделитель
        @param field: Имя поля для объединения
        @type field: str
        @param table: Имя таблицы, которой принадлежит поле
        @type table: str
        @param separator: Разделитель
        @type separator: str

        """
        return "concat_ws('%s', %s)" % (
            separator,
            ", ".join([self.field(f.split("[")[0], f.split("[")[1].rstrip("]")) for f in field.split("+")])
        )

    # noinspection PyUnusedLocal
    def placeholder(self, value, counter):
        """ Возвращает корректный placeholder в зависимости от переданного значения
        :param value:   Значение
        :param counter: Счетчик
        """
        return "%s"

    def limit_skip_section(self, limit_value: int, skip_value: int) -> str:
        """
        Возвращает секцию запроса, отвечающую за ограничение выборки и пропуск записей в результате выборки

        @param limit_value: Ограничение выборки
        @type limit_value: int

        @param skip_value: Количество записей, которое требуется пропустить
        @type skip_value: int

        @return: Секция пропуска записей и ограничение количества возвращаемых записей результата sql-запроса
        @rtype : str

        """
        return "LIMIT %d OFFSET %d" % (limit_value, skip_value) if limit_value else ""

    @staticmethod
    def build_select_query(query: SelectQuery) -> str:
        """
        Строит sql-запрос на выборку строк из таблицы на основе имеющейся информации
        @param query: Экземпляр запроса на выборку строк
        @type query: SelectQuery
        @return : sql-запрос
        @rtype : str

        """
        conditions = query.get_conditions()
        groupby = query.get_groupby_section()
        return '''SELECT %s FROM %s %s %s %s %s ''' % (
            query.get_fields_section(), query.get_table_name(),
            query.get_join_section(),
            "WHERE %s" % conditions if conditions else "",
            "GROUP BY %s" % groupby if len(groupby) > 0 else "",
            query.get_params_section()
        ), query.get_query_data()

    @staticmethod
    def build_delete_query(query: DeleteQuery) -> str:
        """
        Строит запрос на удаление строк из таблицы
        @param query: Экземпляр запроса на удаление строк
        @type query: DeleteQuery
        @return : sql-запрос
        @rtype : str

        """
        conditions = query.get_conditions()
        return '''DELETE %s FROM %s %s %s''' % (
            query.get_table_name(),
            query.get_table_name(),
            query.get_join_section(),
            "WHERE %s" % conditions if conditions else ""
        ), query.get_query_data()

    def build_insert_query(self, query: InsertQuery) -> str:
        """
        Строит запрос на вставку данных в таблицу
        @param query: Экземпляр запроса на вставку данных
        @type query: InsertQuery
        @return: sql-запрос
        @rtype : str
        """
        return '''INSERT INTO %s %s VALUES %s ''' % (
            query.get_table_name(),
            query.get_fields(),
            query.get_placeholders()
        ), query.get_query_data()

    def build_update_query(self, query: UpdateQuery) -> str:
        """
        Строит запрос на обновление данных в таблице по условиям
        @param query: Экземпляр запроса на обновление данных
        @type query: UpdateQuery
        @return: sql-запрос
        @rtype : str

        """
        query.use_table_name_in_set_section()
        assignment = query.get_values_assignment()
        conditions = query.get_conditions()
        return '''UPDATE %s %s SET %s %s %s %s ''' % (
            query.get_table_name(),
            query.get_join_section(),
            assignment,
            "WHERE %s" % conditions if conditions else "",
            query.get_order_section() if query.get_order_section() else "",
            query.get_limit_only_section()
        ), query.get_query_data()


# noinspection PyMethodMayBeStatic
class MsSqlBuilder(SqlBuilder):
    """ Класс, наследующий SqlBuilder и реализующий специфичные для MSSQL особенности синтаксиса SQL """

    def wrap_table(self, table):
        """ Обрамляет кавычками имена таблиц
        :param table:   Имя таблицы
        """
        return '''[%s]''' % table

    def wrap_field(self, field):
        """ Обрамляет кавычками имена полей таблиц
        :param field:   Имя поля
        """
        return '''[%s]''' % field

    def wrap_alias(self, alias):
        """ Обрамляет кавычками алиасы полей
        :param alias:   Имя алиаса
        """
        return '''"%s"''' % alias

    def aggregate_function(self, field: str, table: str, joins: list=None, conditions=None) -> str:
        """
        Возвращает имя аггрегатной функции для группирования строк
        @param field: Имя поля для группировки значений в массив
        @type field: str
        @return: имя аггрегатной функции
        @rtype : str

        """
        conditions = conditions()
        # Для MS SQL коллизия алиасов и имен полей недопустима, поэтому возвращаем имена таблиц
        for j in joins:
            if table == j.alias:
                table = j.foreign_table_name
        join_conditions = " ".join([j.stringify(self) for j in joins])
        return '''STUFF((SELECT DISTINCT ',' + CAST(%s as VARCHAR(MAX)) FROM %s %s %s FOR XML PATH('')), 1, 1, '')''' % (
            field, self.wrap_table(table), join_conditions, ("WHERE %s" % conditions) if conditions else ""
        )

    def concat_ws_function(self, field: str, table: str, separator: str):
        """
        Возвращает функцию конкатенации полей через разделитель
        @param field: Имя поля для объединения
        @type field: str
        @param table: Имя таблицы, которой принадлежит поле
        @type table: str
        @param separator: Разделитель
        @type separator: str

        """

        def get_alias_from_field(f: str):
            if f.endswith("]"):
                f = f.split("[")
                return f[1].replace("]", "")
            else:
                return table

        return "STUFF(%s, 1, 2, '')" % (
            " + ".join([
                '''COALESCE('%s' + CAST(%s as VARCHAR(MAX)), '')''' % (
                    separator, self.field(f.split("[")[0], get_alias_from_field(f))
                )
                for f in field.split("+")]
            )
        )

        # noinspection PyUnusedLocal
    def placeholder(self, value, counter):
        """ Возвращает корректный placeholder в зависимости от переданного значения
        :param value:   Значение
        :param counter: Счетчик
        """
        return "?"

    def limit_skip_section(self, limit_value: int, skip_value: int) -> str:
        """
        Возвращает секцию запроса, отвечающую за ограничение выборки и пропуск записей в результате выборки

        @param limit_value: Ограничение выборки
        @type limit_value: int

        @param skip_value: Количество записей, которое требуется пропустить
        @type skip_value: int

        @return: Секция пропуска записей и ограничение количества возвращаемых записей результата sql-запроса
        @rtype : str

        """
        return "TOP(%d)" % limit_value if limit_value else ""

    @staticmethod
    def build_select_query(query: SelectQuery) -> str:
        """
        Строит sql-запрос на выборку строк из таблицы на основе имеющейся информации
        @param query: Экземпляр запроса на выборку строк
        @type query: SelectQuery
        @return : sql-запрос
        @rtype : str

        """
        conditions = query.get_conditions()
        groupby = query.get_groupby_section()
        return '''SELECT %s %s FROM %s %s %s %s %s ''' % (
            query.get_limit_section(),
            query.get_fields_section(),
            query.get_table_name(),
            query.get_join_section(),
            "WHERE %s" % conditions if conditions else "",
            "GROUP BY %s" % groupby if len(groupby) > 0 else "",
            query.get_order_section() or ""
        ), query.get_query_data()

    @staticmethod
    def build_delete_query(query: DeleteQuery) -> str:
        """
        Строит запрос на удаление строк из таблицы
        @param query: Экземпляр запроса на удаление строк
        @type query: DeleteQuery
        @return : sql-запрос
        @rtype : str

        """
        conditions = query.get_conditions()
        return '''DELETE %s FROM %s %s %s''' % (
            query.get_table_name(),
            query.get_table_name(),
            query.get_join_section(),
            "WHERE %s" % conditions if conditions else ""
        ), query.get_query_data()

    def build_insert_query(self, query: InsertQuery) -> str:
        """
        Строит запрос на вставку данных в таблицу
        @param query: Экземпляр запроса на вставку данных
        @type query: InsertQuery
        @return: sql-запрос
        @rtype : str
        """
        return '''INSERT INTO %s %s VALUES %s ''' % (
            query.get_table_name(),
            query.get_fields(),
            query.get_placeholders()
        ), query.get_query_data()

    def build_update_query(self, query: UpdateQuery) -> str:
        """
        Строит запрос на обновление данных в таблице по условиям
        @param query: Экземпляр запроса на обновление данных
        @type query: UpdateQuery
        @return: sql-запрос
        @rtype : str

        """
        query.use_table_name_in_set_section()
        assignment = query.get_values_assignment()
        conditions = query.get_conditions()
        return '''UPDATE %s %s SET %s FROM %s %s %s ''' % (
            query.get_limit_section(),
            query.get_table_name(),
            assignment,
            query.get_table_name(),
            query.get_join_section(),
            "WHERE %s" % conditions if conditions else ""
        ), query.get_query_data()
