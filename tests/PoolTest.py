from unittest import TestCase
from mapex import Pool, TooManyConnectionsError
from time import time
from mapex.tests.framework.TestFramework import for_all_dbms, DbMock, MsDbMock
from threading import Timer


class PoolTestCase(TestCase):
    @for_all_dbms
    def test_preopen_connections(self, dbms_fw: DbMock):
        """ Опция preopen_connections контроллирует наполнение пула соединениями при инициализации объекта пула """
        lazy_pool = Pool(adapter=dbms_fw.get_adapter(), dsn=dbms_fw.get_dsn(), min_connections=10, preopen_connections=False)
        self.assertEqual(0, lazy_pool.size)

        pool = Pool(adapter=dbms_fw.get_adapter(), dsn=dbms_fw.get_dsn(), min_connections=10)
        self.assertEqual(10, pool.size)

    @for_all_dbms
    def test_with(self, dbms_fw):
        """ Соединение можно получить в with """
        self.assertEqual(2, dbms_fw.pool.size)
        with dbms_fw.pool:
            self.assertEqual(1, dbms_fw.pool.size)
            with dbms_fw.pool:
                self.assertEqual(0, dbms_fw.pool.size)
            self.assertEqual(1, dbms_fw.pool.size)
        self.assertEqual(2, dbms_fw.pool.size)

    @for_all_dbms
    def test_db_property(self, dbms_fw):
        """ Соединение можно получить через свойство пула """
        # Изначально в пуле два соединения
        self.assertEqual(2, dbms_fw.pool.size)

        # Получаем соединение
        db = dbms_fw.pool.db
        self.assertEqual(1, dbms_fw.pool.size)

        # Получаем ещё раз. Это всё то же самое соединение
        connection2 = dbms_fw.pool.db
        self.assertEqual(db, connection2)
        self.assertEqual(1, dbms_fw.pool.size)

        # При удалении соединения оно возвращается в пул
        del dbms_fw.pool.db
        self.assertEqual(2, dbms_fw.pool.size)

    @for_all_dbms
    def test_exhausting_connections(self, dbms_fw: DbMock):
        """ Поведение пула когда заканчиваются соединения с БД """
        # Тест умышленно пропускает базу данных MsSql
        if isinstance(dbms_fw, MsDbMock):
            return

        connection = dbms_fw.get_adapter()()
        connection.connect(dbms_fw.get_dsn())

        # Пул который не хранит в себе соединения может получить соединения только прямо из базы данных
        # Если исчерпать все соединения то получим исключение
        pool_without_min_connections = Pool(dbms_fw.get_adapter(), dbms_fw.get_dsn())
        self.assertRaises(TooManyConnectionsError, self.exhaust_connections, pool_without_min_connections, connection)
        del pool_without_min_connections

        # В обычном случае пул может подождать соединение из пула если закончились соединения с базой данных
        # Имитируем ситуацию когда через 5 секунд в пул возвращается соединение
        # noinspection PyProtectedMember
        Timer(5, lambda: dbms_fw.pool._return_connection(connection)).start()
        self.exhaust_connections(dbms_fw.pool, connection)

    def exhaust_connections(self, pool, connection):
        """ Занимает все соединения с БД """
        t = time()
        with pool as connection2:
            # Если соединения долго не было то оно пришло из пула.
            if time() - t >= 0.5:
                self.assertEqual(connection, connection2)
            else:
                self.exhaust_connections(pool, connection)
