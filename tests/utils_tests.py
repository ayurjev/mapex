""" Тесты библиотеки утилит """
import unittest
from mapex.src.Utils import merge_dict, do_dict


class UtilsTests(unittest.TestCase):
    """ Юниттесты утилит """
    def test_do_dict(self):
        """ Утилита do_dict конструирует словарь по точечной нотации поля """
        self.assertDictEqual({"a": {"b": {"c": 1}}}, do_dict("a.b.c", 1))

    def test_merge_dict(self):
        """ Утилита merge_dict корректно объединяет словари """

        # Случай когда у словарей нет общих ключей
        self.assertDictEqual(
            {
                "a": 1,
                "b": 2
            },
            merge_dict({"a": 1}, {"b": 2})
        )

        # Случай когда у словарей есть общий ключ. Обновляется лишь часть данных
        self.assertDictEqual(
            {
                "a": {
                    "c": 3,
                    "b": 2
                }
            },
            merge_dict({"a": {"c": 3}}, {"a": {"b": 2}})
        )

        # merge_dict() на вход принимает один словарь приёмник и неограниченное количество словарей источников
        self.assertDictEqual(
            {
                "a": 1,
                "b": 2,
                "c": 3
            },
            merge_dict({}, {"a": 1}, {"b": 2}, {"c": 3})
        )

if __name__ == "__main__":
    unittest.main()