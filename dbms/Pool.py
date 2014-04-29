from queue import Queue, Empty
from threading import local as thread_local
from mapex.core.Exceptions import TooManyConnectionsError

class Pool(object):
    """ Пул соединений с базой данных """
    _pool = None
    _min_connections = 0
    _connection_timeout = 5
    _raise_exception = True

    _connector = None
    _dsn = {}

    _instance = None
    _inited = False

    _local = thread_local()

    def __new__(cls, *a, **kwa):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
        return cls._instance

    def kill_instance(self):
        self.__class__._instance = None

    def __init__(self, connector=None, dsn: dict=None, min_connections: int=0):
        """
        Конструктор пула коннектов
        @param connector: коннектор базы данных
        @param dsn: параметры подключения к базе данных
        @param min_connections: число минимально поддерживаемых в пуле соединений
        @return: Pool
        """
        if not self._inited:
            assert min_connections >= 0
            self._inited = True
            self._pool = Queue()
            self._min_connections = min_connections
            self._connector = connector
            self._dsn = dsn
            self._preopen_connections()

    def _open_connection(self):
        """ Возвращает новое соединение к базе данных """
        return self._connector.connect(**self._dsn)

    def _preopen_connections(self):
        """ Наполняет пул минимальным количеством соединений """
        for _ in range(self._min_connections):
            self._free_connection(self._open_connection())

    def _init_connections_container(self):
        if not hasattr(self._local, "connections"):
            self._local.connections = []

    @property
    def connection(self):
        """ Свойство хранит соединение с базой данных которое оно получило из пула """
        if not hasattr(self._local, "connection"):
            self._local.connection = self._get_connection()
        return self._local.connection

    @connection.deleter
    def connection(self):
        """ Возвращает соединение в пул """
        if hasattr(self._local, "connection"):
            self._free_connection(self._local.connection)
            del self._local.connection

    def _get_connection(self):
        """ Пытается получить соединение из пула """
        try:
            return self._pool.get_nowait()
        except Empty:
            # noinspection PyBroadException
            try:
                return self._open_connection()
            except:
                #TODO добавить возможность редактировать свойство класса. Исключение нужно чтобы проходили тесты
                if self._raise_exception:
                    raise TooManyConnectionsError
                else:
                    return self._pool.get()

    def _free_connection(self, connection):
        """ Возвращает соединение в пул либо закрывает его если в пуле нет места """
        if self._pool.qsize() >= self._min_connections:
            connection.close()
        else:
            self._pool.put(connection)

    @property
    def size(self):
        """ Возвращает количество открытых соединений в пуле """
        return self._pool.qsize()

    def __enter__(self):
        if not hasattr(self._local, "connections"):
            self._local.connections = []

        connection = self._get_connection()
        self._local.connections.append(connection)
        return connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._free_connection(self._local.connections.pop())

