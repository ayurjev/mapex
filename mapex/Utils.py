from collections import OrderedDict
from itertools import filterfalse


def partition(predicate, iterable):
    """
    Разбивает iterable на два по условию выполнения функции predicate
    @param predicate: Предикат, принимающий значение из iterable
    @param iterable: Итерируемая коллекция
    @return: Последовательность из двух генераторов (Значения удовлетворяющие предикату, не удовлетворяющие)
    """
    predicate = bool if predicate is None else predicate
    return filter(predicate, iterable), filterfalse(predicate, iterable)


def do_dict(notation, value, cls=OrderedDict) -> dict:
    """
    Рекурсивно строит многомерный словарь по предоставленной точечной нотации
    @param notation: точечная нотация (Пример: user.profile.email)
    @param value: значение поля (Пример: test@test.com)
    @return: словарь с записанным в него значением value (Пример: {"user": {"profile": {"email": "test@test.com"}}})
    """
    head, _, tail = notation.partition(".")
    return cls({head: do_dict(tail, value, cls)}) if tail else cls({head: value})


def merge_dict(dest: dict, *sources: dict, cls=dict) -> dict:
    """
    Рекурсивно обновляет словарь приёмник данными из словарей источников
    @param dest: словарь приёмник
    @param sources: словари источники
    @return:
    """
    for source in sources:
        if isinstance(source, OrderedDict) and not isinstance(dest, OrderedDict):
            dest = OrderedDict(dest)
        for key, value in source.items():
            dest[key] = merge_dict(dest[key] if key in dest.keys() else cls(), value, cls=cls) if isinstance(value, dict) else value
    return dest
