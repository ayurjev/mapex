from unittest import TestCase
from mapex.src.Pool import Pool, TooManyConnectionsError
from time import time
from mapex.tests.framework.TestFramework import for_all_dbms, DbMock, MsDbMock
from threading import Timer


class PoolTestCase(TestCase):
    @for_all_dbms
    def test_preopen_connections(self, dbms_fw: DbMock):
        """ При создании пул создаёт необходимое количество соединений """
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
