
from abc import ABCMeta, abstractmethod


class TrackChangesValue(object):
    @abstractmethod
    def is_changed(self):
        """ Возвращает признак того, изменился ли объект """


class ValueInside(object):
    @abstractmethod
    def get_value(self):
        """ Возвращает значение, хранящееся в объекте """