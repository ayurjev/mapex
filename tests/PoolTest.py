from unittest import TestCase
from mapex.dbms.Pool import Pool
from mapex.dbms.Adapters import MySqlDbAdapter


class PoolTestCase(TestCase):
    dsn = ("127.0.0.1", "3306", "unittests", "", "unittests")

    def setUp(self):
        self.pool = Pool(adapter=MySqlDbAdapter, dsn=self.dsn, min_connections=2)

    def test_preopen_connections(self):
        """ Опция preopen_connections контроллирует наполнение пула соединениями при инициализации объекта пула """
        lazy_pool = Pool(adapter=MySqlDbAdapter, dsn=self.dsn, min_connections=10, preopen_connections=False)
        self.assertEqual(0, lazy_pool.size)

        pool = Pool(adapter=MySqlDbAdapter, dsn=self.dsn, min_connections=10)
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

    def test_db_property(self):
        """ Соединение можно получить через свойство пула """
        # Изначально в пуле два соединения
        self.assertEqual(2, self.pool.size)

        # Получаем соединение
        db = self.pool.db
        self.assertEqual(1, self.pool.size)

        # Получаем ещё раз. Это всё то же самое соединение
        connection2 = self.pool.db
        self.assertEqual(db, connection2)
        self.assertEqual(1, self.pool.size)

        # При удалении соединения оно возвращается в пул
        del self.pool.db
        self.assertEqual(2, self.pool.size)

    def test_exhausting_pool(self):
        """
        Пул выделяет коннекты пока есть возможность их создавать
        Когда создать соединение невозможно пул ждёт пока в него вернут одно из уже открытых соединений
        """

        connection = MySqlDbAdapter()
        connection.connect(self.dsn)

        def exhaust_pool():
            """ Вычерпывает пул коннектов """
            from time import time
            t = time()
            with self.pool as connection2:
                # Если соединения долго не было то оно пришло из пула.
                # Проверяю что это соединение возвращённое таймером
                if time() - t >= 0.5:
                    self.assertEqual(connection, connection2)
                    return

                exhaust_pool()

        def free_connection():
            """  Возвращает коннект в пул """
            # noinspection PyProtectedMember
            self.pool._free_connection(connection)

        from threading import Timer
        # Через секунду в пул вернётся соединение
        Timer(1, free_connection).start()
        # К этому моменту все соединения к базе данных уже будут заняты и пул будет ждать возвращения соединений
        exhaust_pool()
