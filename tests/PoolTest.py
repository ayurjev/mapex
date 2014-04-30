from mysql import connector as mysql_connector
from unittest import TestCase
from mapex.dbms.Pool import Pool, TooManyConnectionsError


class PoolTestCase(TestCase):
    dsn = {"user": "unittests", "host": "127.0.0.1", "port": "3306", "database": "test", "autocommit": True}
    pool = Pool(connector=mysql_connector, dsn=dsn, min_connections=2, preopen_connections=True)

    def test_preopen_connections(self):
        """ Опция preopen_connections контроллирует наполнение пула соединениями при инициализации объекта пула """
        lazy_pool = Pool(connector=mysql_connector, dsn=self.dsn, min_connections=10, preopen_connections=False)
        self.assertEqual(0, lazy_pool.size)

        pool = Pool(connector=mysql_connector, dsn=self.dsn, min_connections=10, preopen_connections=True)
        self.assertEqual(10, pool.size)

    def test_with(self):
        """ Соединение можно получить в with """
        self.assertEqual(2, self.pool.size)
        with self.pool:
            self.assertEqual(1, self.pool.size)
            with self.pool:
                self.assertEqual(0, self.pool.size)
            self.assertEqual(1, self.pool.size)

        self.assertEqual(2, self.pool.size)

    def test_connection_property(self):
        """ Соединение можно получить через свойство пула """
        # Изначально в пуле два соединения
        self.assertEqual(2, self.pool.size)

        # Получаем соединение
        connection = self.pool.connection
        self.assertEqual(1, self.pool.size)

        # Получаем ещё раз. Это всё то же самое соединение
        connection2 = self.pool.connection
        self.assertEqual(connection, connection2)
        self.assertEqual(1, self.pool.size)

        # При удалении соединения оно возвращается в пул
        del self.pool.connection
        self.assertEqual(2, self.pool.size)

    def test_exhausting_pool(self):
        """
        Пул выделяет коннекты пока есть возможность их создавать
        Когда создать соединение невозможно пул выдаёт исключение QueueEmpty
        @return:
        """
        def exhaust_pool():
            """ Вычерпывает пул коннектов """
            with self.pool:
                exhaust_pool()

        self.assertRaises(TooManyConnectionsError, exhaust_pool)
