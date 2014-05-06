from queue import Queue, Empty
from threading import local
from mapex.core.Sql import Adapter


class TooManyConnectionsError(Exception):
    """ Нет возможности создать подключение к базе данных из-за превышения ограничения на количество соединений с БД """


class Pool(object):
    """ Пул адаптеров баз данных """
    def __init__(self, adapter: type, dsn: tuple, min_connections: int=0, preopen_connections: bool=True):
        """
        Конструктор пула
        @param adapter: класс адаптера
        @param dsn: параметры подключения к БД
        @param min_connections: число минимально поддерживаемых в пуле соединений
        @param preopen_connections: надо заполнить пул готовыми соединениями
        @return: Pool
        """
        assert min_connections >= 0
        self._pool = Queue()
        self._adapter = adapter
        self._dsn = dsn
        self._min_connections = min_connections
        self._pool_wait_timeout = 1

        self._local = local()

        if preopen_connections:
            self._preopen_connections()

    @property
    def _new_connection(self) -> Adapter:
        """ Новое соединение к базе данных или False
        @return: Adapter | False
        """
        return self._adapter().connect(self._dsn)

    @property
    def _connection_from_pool(self):
        """ Соединение из пула или False
        @return: Adapter | False
        """
        try:
            return self._pool.get_nowait()
        except Empty:
            return False

    @property
    def _wait_connection_from_pool(self):
        """ Пытаемся немного подождать соединение из пула """
        if self._min_connections == 0:
            raise TooManyConnectionsError()

        try:
            return self._pool.get(timeout=self._pool_wait_timeout)
        except Empty:
            return False

    def _get_connection(self) -> Adapter:
        """ Просит соединение у одного из источников и отправляется на следующий виток если никто ничего не вернул """
        return self._connection_from_pool\
            or self._new_connection\
            or self._wait_connection_from_pool\
            or self._get_connection()

    def _return_connection(self, db: Adapter):
        """ Возвращает соединение в пул если оно ещё нужно иначе закрывает его """
        if self.size < self._min_connections:
            self._pool.put(db)
        else:
            db.close()

    def _preopen_connections(self, opened=0):
        """ Наполняет пул минимальным количеством соединений """
        if opened < self._min_connections:
            with self:
                self._preopen_connections(opened + 1)

    @property
    def db(self):
        """ Свойство хранит соединение с базой данных """
        #TODO проверять состояние соединения и `del self._local.connection` если соединение умерло
        if not hasattr(self._local, "connection"):
            self._local.connection = self._get_connection()
        return self._local.connection

    @db.deleter
    def db(self):
        """ Освобождает соединение и возвращает в пул """
        if hasattr(self._local, "connection"):
            self._return_connection(self._local.connection)
            del self._local.connection

    @property
    def size(self):
        """ Количество готовых соединений в пуле """
        return self._pool.qsize()

    def __enter__(self):
        """ На входе получает из пула новое соединение и запоминает в локальной переменной потока """
        if not hasattr(self._local, "connections"):
            self._local.connections = []

        self._local.connections.append(self._get_connection())
        return self._local.connections[-1]

    def __exit__(self, *args):
        """ На выходе возвращает соединение в пул """
        self._return_connection(self._local.connections.pop())