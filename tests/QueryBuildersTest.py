""" Модульные тесты для классов SQL-синтаксиса разных баз данных """

import unittest
import re
from collections import OrderedDict
from mapex.QueryBuilders import PgSqlBuilder, MySqlBuilder, MsSqlBuilder,  \
    SelectQuery, DeleteQuery, InsertQuery, UpdateQuery
from mapex.Sql import PlaceHoldersCounter
from mapex.Mappers import Join


class QueryBuildersTest(unittest.TestCase):
    """ Модульные тесты для классов построения sql-запросов """

    def test_select(self):
        """ Проверим создание запроса на выборку данных из таблицы """

        self.check_select(
            PgSqlBuilder(),
            '''
            SELECT "mainTable"."FirstField", "mainTable"."SecondField"
            FROM "mainTable"
            LEFT JOIN "secondTable" as "secondTable" ON ("mainTable"."FirstField" = "secondTable"."ID")
            WHERE ("mainTable"."SecondField" = $1 AND "mainTable"."FirstField" = $2)
            ORDER BY "mainTable"."FirstField" ASC, "secondTable"."ID" DESC LIMIT 10 OFFSET 50
            ''',
            ["aaa", 1]
        )

        self.check_select(
            MySqlBuilder(),
            '''
            SELECT `mainTable`.`FirstField`, `mainTable`.`SecondField`
            FROM `mainTable`
            LEFT JOIN `secondTable` as `secondTable` ON (`mainTable`.`FirstField` = `secondTable`.`ID`)
            WHERE (`mainTable`.`SecondField` = %s AND `mainTable`.`FirstField` = %s)
            ORDER BY `mainTable`.`FirstField` ASC, `secondTable`.`ID` DESC LIMIT 10 OFFSET 50
            ''',
            ["aaa", 1]
        )

        self.check_select(
            MsSqlBuilder(),
            '''
            SELECT TOP(10) [mainTable].[FirstField], [mainTable].[SecondField]
            FROM [mainTable]
            LEFT JOIN [secondTable] as [secondTable] ON ([mainTable].[FirstField] = [secondTable].[ID])
            WHERE ([mainTable].[SecondField] = ? AND [mainTable].[FirstField] = ?)
            ORDER BY [mainTable].[FirstField] ASC, [secondTable].[ID] DESC
            ''',
            ["aaa", 1]
        )

    def test_delete(self):
        """ Проверим создание запроса на удаление данных из таблицы """

        self.check_delete(
            PgSqlBuilder(),
            '''
            DELETE FROM "mainTable"
            USING "secondTable" as "secondTable"
            WHERE
            ("mainTable"."SecondField" = $1 AND "mainTable"."FirstField" = $2) AND
            ("mainTable"."FirstField" = "secondTable"."ID")
            ''',
            ["aaa", 1]
        )

        self.check_delete(
            MySqlBuilder(),
            '''
            DELETE `mainTable` FROM `mainTable`
            LEFT JOIN `secondTable` as `secondTable` ON (`mainTable`.`FirstField` = `secondTable`.`ID`)
            WHERE
            (`mainTable`.`SecondField` = %s AND `mainTable`.`FirstField` = %s)
            ''',
            ["aaa", 1]
        )

    def test_insert(self):
        """ Проверим создание запроса на вставку данных в таблицу """
        item1 = OrderedDict()
        item1["ID"] = 1
        item1["Name"] = "aaa"
        item2 = OrderedDict()
        item2["ID"] = 2
        item2["Name"] = "bbb"

        self.check_insert(
            PgSqlBuilder(), item1,
            '''INSERT INTO "mainTable" ("ID", "Name") VALUES ($1, $2) RETURNING "mainTable"."ID"''', [1, "aaa"]
        )

        self.check_insert(
            PgSqlBuilder(), [item1, item2],
            '''INSERT INTO "mainTable" ("ID", "Name") VALUES ($1, $2), ($3, $4) RETURNING "mainTable"."ID"''',
            [1, "aaa", 2, "bbb"]
        )

        self.check_insert(
            MySqlBuilder(), item1,
            '''INSERT INTO `mainTable` (`ID`, `Name`) VALUES (%s, %s)''', [1, "aaa"]
        )
        self.check_insert(
            MySqlBuilder(), [item1, item2],
            '''INSERT INTO `mainTable` (`ID`, `Name`) VALUES (%s, %s), (%s, %s)''', [1, "aaa", 2, "bbb"]
        )

    def test_update(self):
        """ Проверим создание запроса на обновление данных в таблице """
        self.check_update(
            PgSqlBuilder(),
            '''
            UPDATE "mainTable" *
            SET "ID" = $1, "Name" = $2
            FROM "secondTable" as "secondTable"
            WHERE
            ("mainTable"."ID" = $3) AND
            ("mainTable"."FirstField" = "secondTable"."ID")
            RETURNING "mainTable"."ID"
            ''',
            [9, "NewName", 1]
        )

        self.check_update(
            MySqlBuilder(),
            '''
            UPDATE `mainTable`
            LEFT JOIN `secondTable` as `secondTable` ON (`mainTable`.`FirstField` = `secondTable`.`ID`)
            SET `mainTable`.`ID` = %s, `mainTable`.`Name` = %s
            WHERE (`mainTable`.`ID` = %s)
            ''',
            [9, "NewName", 1]
        )

        self.check_update(
            MsSqlBuilder(),
            '''
            UPDATE [mainTable]
            SET [mainTable].[ID] = ?, [mainTable].[Name] = ?
            FROM [mainTable]
            LEFT JOIN [secondTable] as [secondTable] ON ([mainTable].[FirstField] = [secondTable].[ID])
            WHERE ([mainTable].[ID] = ?)
            ''',
            [9, "NewName", 1]
        )


################################## Список полей #######################################

    def test_fields_enumerations(self):
        """ Простое перечисление полей """
        self.assertEqual(
            '''"FirstField", "SecondField", "Date"''',
            PgSqlBuilder().fields_enumeration(["FirstField", "SecondField", "Date"])
        )

    def test_fields_enumeratoin_with_aliases(self):
        """ Перечисление полей с алиасами """
        self.assertEqual(
            '''"FirstField" as "fieldOne", "SecondField" as "fieldTwo", "Date"''',
            PgSqlBuilder().fields_enumeration([("FirstField", "fieldOne"), ("SecondField", "fieldTwo"), "Date"])
        )

    def test_fields_enumerations_with_table_name(self):
        """ Перечисление полей с указанием имени таблицы """
        self.assertEqual(
            '''"MainTable"."FirstField" as "fieldOne", "SecondTable"."SecondField" as "fieldTwo", "MainTable"."Date"''',
            PgSqlBuilder().fields_enumeration(
                [("FirstField", "fieldOne"), ("SecondTable.SecondField", "fieldTwo"), "Date"],
                "MainTable"
            )
        )

    def test_fields_enumerations_with_table_name_but_no_aliases(self):
        """ Перечисление полей с указанием имени таблицы, но без алиасов """
        self.assertEqual(
            '''"MainTable"."FirstField", "SecondTable"."SecondField"''',
            PgSqlBuilder().fields_enumeration(["FirstField", "SecondTable.SecondField"], "MainTable")
        )

    ################################## Сравнение полей #######################################

    def test_field_comparisons_eq(self):
        """ Сравнение полей на равенство """
        res = PgSqlBuilder().fields_comparisons(
            {"FirstField": "aaa", "SecondField": ("e", "bbb")}, PlaceHoldersCounter()
        )
        self.assertTrue(
            res == ('''("FirstField" = $1 AND "SecondField" = $2)''', ["aaa", "bbb"]) or
            ('"SecondField" = $1 AND "FirstField" = $2', ['bbb', 'aaa'])
        )

    def test_field_comparisons_not_eq(self):
        """ Сравнение полей на неравенство """
        res = PgSqlBuilder().fields_comparisons({"SecondField": ("ne", "bbb")}, PlaceHoldersCounter())
        self.assertEqual(('("SecondField" != $1)', ['bbb']), res)

    def test_field_comparisons_eq_aliases(self):
        """ Сравнение полей на равенство с использованием алиасов """
        res = PgSqlBuilder().fields_comparisons(
            {"FirstField": "aaa", "SecondTable.SecondField": "bbb"}, PlaceHoldersCounter(), "MainTable"
        )
        opt1 = res == ('''("MainTable"."FirstField" = $1 AND "SecondTable"."SecondField" = $2)''', ["aaa", "bbb"])
        opt2 = res == ('("SecondTable"."SecondField" = $1 AND "MainTable"."FirstField" = $2)', ['bbb', 'aaa'])
        self.assertTrue(opt1 or opt2)

    def test_field_comparisons_ltgt(self):
        """ Сравнение полей больше-меньше """
        res = PgSqlBuilder().fields_comparisons(
            {"FirstField": ("gt", 2), "SecondField": ("lt", 5)}, 
            PlaceHoldersCounter()
        )
        opt1 = res == ('''("FirstField" > $1 AND "SecondField" < $2)''', [2, 5])
        opt2 = res == ('''("SecondField" < $1 AND "FirstField" > $2)''', [5, 2])
        self.assertTrue(opt1 or opt2)

    def test_field_comparisons_ltegte(self):
        """ Сравнение полей больше/равно-меньше/равно """
        res = PgSqlBuilder().fields_comparisons(
            {"FirstField": ("gte", 2), "SecondField": ("lte", 5)}, 
            PlaceHoldersCounter()
        )
        opt1 = res == ('''("FirstField" >= $1 AND "SecondField" <= $2)''', [2, 5])
        opt2 = res == ('''("SecondField" <= $1 AND "FirstField" >= $2)''', [5, 2])
        self.assertTrue(opt1 or opt2)

    def test_field_comparisons_eq_same_key(self):
        """ Сравнение полей  - дважды используется одно и тоже поле - объединяем через and """
        res = PgSqlBuilder().fields_comparisons(
            {"and": [{"FirstField": ("gt", 2)}, {"FirstField": ("lt", 5)}]}, 
            PlaceHoldersCounter()
        )
        opt1 = res == ('''((("FirstField" > $1) AND ("FirstField" < $2)))''', [2, 5])
        opt2 = res == ('''((("FirstField" < $1) AND ("FirstField" > $2)))''', [5, 2])
        self.assertTrue(opt1 or opt2)

    def test_field_comparisons_or(self):
        """ Сравнение полей  - дважды используется одно и тоже поле - объединяем через or """
        res = PgSqlBuilder().fields_comparisons(
            {"or": [{"FirstField": ("gt", 2)}, {"FirstField": ("lt", 5)}]}, 
            PlaceHoldersCounter()
        )
        opt1 = res == ('''((("FirstField" > $1) OR ("FirstField" < $2)))''', [2, 5])
        opt2 = res == ('''((("FirstField" < $1) OR ("FirstField" > $2)))''', [5, 2])
        self.assertTrue(opt1 or opt2)

    def test_field_comparisons_and_with_or_combined(self):
        """ Используем и and- и or-объединение при сравнении полей """
        res = PgSqlBuilder().fields_comparisons(
            {
                "and": [
                    {
                        "FirstField": ("gt", 2), "or": [
                            {"SecondField": "aaa", "ThirdField": "bbb"},
                            {"SecondField": ("lt", 10)}
                        ]
                    }
                ]
            },
            PlaceHoldersCounter()
        )
        opt1 = res == (
            '''((("FirstField" > $1 AND (("SecondField" = $2 AND "ThirdField" = $3) OR ("SecondField" < $4)))))''',
            [2, "aaa", "bbb", 10]
        )
        opt2 = res == (
            '''((("FirstField" > $1 AND (("ThirdField" = $2 AND "SecondField" = $3) OR ("SecondField" < $4)))))''',
            [2, "bbb", "aaa", 10]
        )
        opt3 = res == (
            '''((((("SecondField" = $1 AND "ThirdField" = $2) OR ("SecondField" < $3)) AND "FirstField" > $4)))''',
            ['aaa', 'bbb', 10, 2]
        )
        opt4 = res == (
            '''((((("ThirdField" = $1 AND "SecondField" = $2) OR ("SecondField" < $3)) AND "FirstField" > $4)))''',
            ['bbb', 'aaa', 10, 2]
        )
        self.assertTrue(opt1 or opt2 or opt3 or opt4)

    def test_field_comparisons_in(self):
        """ Проверка на вхождение в диапазон """
        res = PgSqlBuilder().fields_comparisons({"FirstField": ("in", [1, 2, 3])}, PlaceHoldersCounter())
        self.assertEqual(('''("FirstField" IN ($1, $2, $3))''', [1, 2, 3]), res)

        self.query = PgSqlBuilder()
        res = PgSqlBuilder().fields_comparisons({"FirstField": ("nin", [1, 2, 3])}, PlaceHoldersCounter())
        self.assertEqual(('''("FirstField" NOT IN ($1, $2, $3))''', [1, 2, 3]), res)

    def test_field_comparisons_exists(self):
        """ Проверка, что поле не NULL """
        res = PgSqlBuilder().fields_comparisons({"FirstField": ("exists", True)}, PlaceHoldersCounter())
        self.assertEqual(('''("FirstField" IS NOT NULL)''', []), res)

        res = PgSqlBuilder().fields_comparisons({"FirstField": ("exists", False)}, PlaceHoldersCounter())
        self.assertEqual(('''("FirstField" IS NULL)''', []), res)

    def test_field_comparisons_match(self):
        """ Проверка, что поле соответствует маске """
        res = PgSqlBuilder().fields_comparisons({"FirstField": ("match", "a*c")}, PlaceHoldersCounter())
        self.assertEqual(('''("FirstField" LIKE $1)''', ['a%c']), res)

    ################################ Вспомогательные функции #########################################################

    @staticmethod
    def unittest_trim(string):
        """ Убирает из строки переводы строк, табуляцию и повторяющиеся пробелы... """
        string = string.replace("\t", "  ").replace("\n", "  ").replace("\r", "  ")
        string = re.sub(">\s\s+", ">", string)
        string = re.sub("\s\s+<", "<", string)
        string = re.sub("\s\s+", " ", string, flags=re.MULTILINE)
        string = string.strip(" ")
        return string

    def check_select(self, sql_builder, expected_sql, expected_data):
        """
        Заготовка для тестирования SELECT-запросов
        :param sql_builder:
        :param expected_sql:
        :param expected_data:
        :return:
        """
        query = SelectQuery(sql_builder)
        query.set_table_name("mainTable")
        query.set_fields(["FirstField", "SecondField"])
        conditions = OrderedDict()
        conditions["SecondField"] = "aaa"
        conditions["FirstField"] = 1
        query.set_conditions(conditions)
        query.set_joins([Join("secondTable", "mainTable", "FirstField", "secondTable", "ID")])
        query.set_params({
            "order": [("FirstField", "ASC"), ("secondTable.ID", "DESC")],
            "limit": 10,
            "skip": 50
        })
        sql, data = sql_builder.build_select_query(query)
        self.assertEqual(
            (self.unittest_trim(expected_sql), expected_data),
            (self.unittest_trim(sql), data)
        )

    def check_delete(self, sql_builder, expected_sql, expected_data):
        """
        Заготовка для тестирования DELETE-запросов
        :param sql_builder:
        :param expected_sql:
        :param expected_data:
        :return:
        """
        query = DeleteQuery(sql_builder)
        query.set_table_name("mainTable")
        conditions = OrderedDict()
        conditions["SecondField"] = "aaa"
        conditions["FirstField"] = 1
        query.set_conditions(conditions)
        query.set_joins([Join("secondTable", "mainTable", "FirstField", "secondTable", "ID")])
        sql, data = sql_builder.build_delete_query(query)
        self.assertEqual(
            (self.unittest_trim(expected_sql), expected_data),
            (self.unittest_trim(sql), data)
        )

    def check_insert(self, sql_builder, data, expected_sql, expected_data):
        """
        Заготовка для тестирования INSERT-запросов
        :param sql_builder:
        :param expected_sql:
        :param expected_data:
        :return:
        """
        query = InsertQuery(sql_builder)
        query.set_table_name("mainTable")
        query.set_insert_data(data)
        query.set_primary("ID")
        sql, data = sql_builder.build_insert_query(query)
        self.assertEqual(
            (self.unittest_trim(expected_sql), expected_data),
            (self.unittest_trim(sql), data)
        )

    def check_update(self, sql_builder, expected_sql, expected_data):
        """
        Заготовка для тестирования UPDATE-запросов
        :param sql_builder:
        :param expected_sql:
        :param expected_data:
        :return:
        """
        query = UpdateQuery(sql_builder)
        query.set_table_name("mainTable")
        query.set_conditions({"ID": 1})
        query.set_joins([Join("secondTable", "mainTable", "FirstField", "secondTable", "ID")])
        data = OrderedDict()
        data["ID"] = 9
        data["Name"] = "NewName"
        query.set_update_data(data)
        query.set_primary("ID")
        sql, data = sql_builder.build_update_query(query)
        self.assertEqual(
            (self.unittest_trim(expected_sql), expected_data),
            (self.unittest_trim(sql), data)
        )


if __name__ == '__main__':
    unittest.main()