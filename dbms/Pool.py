from queue import Queue, Empty
from threading import local


class Pool(object):
    """ Пул соединений с базой данных """
    def __init__(self, connector=None, dsn: dict=None, min_connections: int=0, preopen_connections=True):
        """
        Конструктор пула коннектов
        @param connector: коннектор базы данных
        @param dsn: параметры подключения к базе данных
        @param min_connections: число минимально поддерживаемых в пуле соединений
        @param preopen_connections: надо заполнить пул готовыми соединениями
        @return: Pool
        """
        assert min_connections >= 0
        self._pool = Queue()
        self._connector = connector
        self._dsn = dsn
        self._min_connections = min_connections
        self._raise_exception = True

        self._local = local()
        self._local.connections = []
        self._local.connection = None

        if preopen_connections:
            self._preopen_connections()

    def _open_connection(self):
        """ Возвращает новое соединение к базе данных """
        return self._connector.connect(**self._dsn)

    def _preopen_connections(self):
        """ Наполняет пул минимальным количеством соединений """
        for _ in range(self._min_connections):
            self._free_connection(self._open_connection())

    @property
    def connection(self):
        """ Свойство хранит соединение с базой данных которое оно получило из пула """
        if self._local.connection is None:
            self._local.connection = self._get_connection()
        return self._local.connection

    @connection.deleter
    def connection(self):
        """ Возвращает соединение в пул """
        if self._local.connection is not None:
            self._free_connection(self._local.connection)
            self._local.connection = None

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
        """ Возвращает количество готовых соединений """
        return self._pool.qsize()

    def __enter__(self):
        """ На входе получает из пула новое соединение и запоминает в локальном списке """
        self._local.connections.append(self._get_connection())
        return self._local.connections[-1]

    def __exit__(self, *args):
        """ На выходе возвращает соединение в пул """
        self._free_connection(self._local.connections.pop())

    def __del__(self):
        """ Пул закрывает все занятые соединения """
        if self._local.connection:
            self._local.connection.close()

        if self._local.connections:
            for connection in self._local.connections:
                connection.close()

        while True:
            try:
                self._pool.get_nowait().close()
            except Empty:
                break


class TooManyConnectionsError(Exception):
    """ Превышено ограничение количества соединений с базой данных """
    pass
