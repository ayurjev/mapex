"""
Исключения, которые могут возникать при работе с Mapex
"""


class DublicateRecordException(Exception):
    pass


class AdapterException(Exception):
    """ Исключение работы на уровне адаптера базы данных """
    pass


class TableMapperException(Exception):
    """ Класс исключений, связанных с ошибками уровня маппера """
    pass


class TableModelException(Exception):
    """ Класс исключений, связанных с ошибками работы коллекций модели """
    pass


class RecordModelException(Exception):
    """ Класс исключений, связанных с ошибками работы модели """
    pass


class EmbeddedObjectFactoryException(Exception):
    """ Класс исключений, связанных с ошибками при работе с EmbeddedObjectFactory """
    pass
