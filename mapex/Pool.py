from queue import Queue, Empty
from threading import local

from .Sql import Adapter
from .Models import Transaction


class TooManyConnectionsError(Exception):
    """ Нет возможности создать подключение к базе данных из-за превышения ограничения на количество соединений с БД """


class Pool(object):
    """ Пул адаптеров баз данных """
    def __init__(self, adapter: type, dsn: tuple, min_connections: int=1):
        """
        Конструктор пула
        @param adapter: класс адаптера
        @param dsn: параметры подключения к БД
        @param min_connections: число минимально поддерживаемых в пуле соединений
        @return: Pool
        """
        assert min_connections >= 0
        assert dsn

        self._pool = Queue()
        self._adapter = adapter
        self._dsn = dsn
        self._min_connections = min_connections
        self._local = local()

    @property
    def in_transaction(self):
        if not hasattr(self._local, "in_transaction"):
            self._local.in_transaction = False
        return self._local.in_transaction

    @in_transaction.setter
    def in_transaction(self, value):
        if not hasattr(self._local, "in_transaction"):
            self._local.in_transaction = False
        self._local.in_transaction = True if value else False

    def _new_connection(self, autocommit=True) -> Adapter:
        """ Новое соединение к базе данных или False
        @return: Adapter | False
        """
        return self._adapter().connect(self._dsn, autocommit)

    @property
    def _connection_from_pool(self):
        """ Соединение из пула или False
        @return: Adapter | False
        """
        try:
            return self._pool.get_nowait()
        except Empty:
            return False

    def _get_connection(self) -> Adapter:
        """ Берёт соединение из пула или открывает новое если пул пуст """
        return self._connection_from_pool or self._new_connection()

    def _return_connection(self, db: Adapter):
        """ Возвращает соединение в пул если оно ещё нужно иначе закрывает его """
        self._pool.put(db)

    def _preopen_connections(self):
        """ Наполняет пул минимальным количеством соединений """
        for i in range(self._min_connections):
            self._return_connection(self._new_connection())

    @property
    def _local_tx_connection(self):
        if not hasattr(self._local, "tx_connection"):
            self._local.tx_connection = self._new_connection(autocommit=False)
        return self._local.tx_connection

    @property
    def _local_connection(self):
        if not hasattr(self._local, "connection"):
            self._local.connection = self._get_connection()
        return self._local.connection

    @property
    def db(self):
        """ Свойство хранит соединение с базой данных """
        return self._local_tx_connection if self.in_transaction else self._local_connection

    @db.deleter
    def db(self):
        """ Освобождает соединение и возвращает в пул """
        if hasattr(self._local, "tx_connection"):
            del self._local.tx_connection

        if hasattr(self._local, "connection"):
            self._return_connection(self._local.connection)
            del self._local.connection

    @property
    def transaction(self) -> Transaction:
        return Transaction(self)

    @property
    def size(self):
        """ Количество готовых соединений в пуле """
        return self._pool.qsize()

    def __enter__(self):
        """ На входе получает из пула новое соединение и запоминает в локальной переменной потока """
        if self.in_transaction:
            return self.db

        if not hasattr(self._local, "connections"):
            self._local.connections = []

        self._local.connections.append(self._get_connection())
        return self._local.connections[-1]

    def __exit__(self, *args):
        """ На выходе возвращает соединение в пул """
        if not self.in_transaction:
            self._return_connection(self._local.connections.pop())
