from mysql import connector as mysql_connector
from unittest import TestCase
from mapex.dbms.Pool import Pool


class PoolTestCase(TestCase):
    dsn = {"user": "unittests", "host": "127.0.0.1", "port": "3306", "database": "test", "autocommit": True}

    def setUp(self):
        self.pool = Pool(connector=mysql_connector, dsn=self.dsn, min_connections=2, preopen_connections=True)

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
        Когда создать соединение невозможно пул ждёт пока в него вернут одно из уже открытых соединений
        """

        connection = self.pool.connection
        # Мы вручную вернём коннект в пул (код исключительно для теста)
        # noinspection PyProtectedMember
        del self.pool._local.connection

        def exhaust_pool():
            """ Вычерпывает пул коннектов """
            from time import time
            t = time()
            with self.pool as connection2:
                # Пришло соединение из БД или из пула можно понять по затраченному времени
                if time() - t >= 0.5:
                    self.assertEqual(connection, connection2)
                    return

                exhaust_pool()

        def free_connection():
            """  Возвращает коннект в пул """
            self.pool._free_connection(connection)

        from threading import Timer
        # Через секунду в пул вернётся соединение
        Timer(1, free_connection).start()
        # К этому моменту все соединения к базе данных уже будут заняты и пул будет ждать возвращения соединений
        exhaust_pool()
