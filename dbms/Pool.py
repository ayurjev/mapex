from queue import Queue, Empty
from threading import local
from mapex.dbms.Adapters import Adapter


class Pool(object):
    """ Пул адаптеров баз данных """
    def __init__(self, adapter: type, dsn: tuple, min_connections: int=0, preopen_connections=True):
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
        self._raise_exception = True

        self._local = local()

        if preopen_connections:
            self._preopen_connections()

    def _open_connection(self) -> Adapter:
        """ Подключение к БД через адаптер """
        db = self._adapter()
        db.connect(self._dsn)
        return db

    def _get_pool_connection(self) -> Adapter:
        """ Пытается получить соединение из пула """
        try:
            return self._pool.get_nowait()
        except Empty:
            # noinspection PyBroadException
            try:
                return self._open_connection()
            except:
                return self._pool.get()

    def _preopen_connections(self, opened=0):
        """ Наполняет пул минимальным количеством соединений """
        if opened < self._min_connections:
            with self:
                self._preopen_connections(opened + 1)

    def _free_connection(self, db: Adapter):
        """ Возвращает соединение в пул если оно ещё нужно иначе закрывает его """
        if self.size < self._min_connections:
            self._pool.put(db)
        else:
            db.close()

    @property
    def db(self):
        """ Свойство хранит соединение с базой данных """
        #TODO проверять состояние соединения и `del self._local.connection` если соединение умерло
        if not hasattr(self._local, "connection"):
            self._local.connection = self._get_pool_connection()
        return self._local.connection

    @db.deleter
    def db(self):
        """ Освобождает соединение и возвращает в пул """
        if hasattr(self._local, "connection"):
            self._free_connection(self._local.connection)
            del self._local.connection

    @property
    def size(self):
        """ Возвращает количество готовых соединений """
        return self._pool.qsize()

    def __enter__(self):
        """ На входе получает из пула новое соединение и запоминает в локальном списке """
        if not hasattr(self._local, "connections"):
            self._local.connections = []

        self._local.connections.append(self._get_pool_connection())
        return self._local.connections[-1]

    def __exit__(self, *args):
        """ На выходе возвращает соединение в пул """
        self._free_connection(self._local.connections.pop())

    def __del__(self):
        """ Пул закрывает все занятые соединения """
        if hasattr(self._local, "connection"):
            self._local.connection.close()
            del self._local.connection

        if hasattr(self._local, "connections"):
            for db in self._local.connections:
                db.close()
            self._local.connections = []

        while True:
            try:
                connection = self._pool.get_nowait()
                connection.close()
            except Empty:
                break
