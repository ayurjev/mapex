"""
Тесты для библиотеки для работы с базами данных
Уровень моделей предметной области

"""

import unittest
from datetime import date

from mapex.tests.framework.TestFramework import for_all_dbms
from mapex.core.Exceptions import TableModelException


class TableModelTest(unittest.TestCase):
    """ Модульные тесты для класса TableModel """

    def setUp(self):
        # noinspection PyPep8Naming
        self.maxDiff = None

    @for_all_dbms
    def test_count_insert_delete_update(self, dbms_fw):
        """ Проверим базовые методы работы с моделью как с коллекцией """
        users = dbms_fw.get_new_users_collection_instance()
        self.assertEqual(0, users.count())

        users.insert({"name": "third"})
        self.assertEqual(1, users.count())

        users.insert([
            {"age": 99, "name": "valueForFieldName1"},
            {"age": 999, "name": "valueForFieldName2"}
        ])
        self.assertEqual(3, users.count())

        users.delete({"age": ("in", [99, 999])})
        self.assertEqual(1, users.count())

        users.delete()
        self.assertEqual(0, users.count())

        uid = users.insert({"name": "InitialValue"})
        users.update({"name": "NewValue"}, {"uid": uid})
        self.assertEqual(0, users.count({"name": "InitalValue"}))
        self.assertEqual(1, users.count({"name": "NewValue"}))

    @for_all_dbms
    def test_advanced_insert_behavior(self, dbms_fw):
        """ Проверим также всю возможную логику при вставке данных """
        # Во первых коллекция принимает либо словарь с данными, соответствующими модели, либо корректную модель
        # либо список словарей с данными, соответствующими модели, либо список корректных моделей
        # Вставка данных корректными словарями тестируется в test_count_insert_delete_update()
        users = dbms_fw.get_new_users_collection_instance()
        self.assertEqual(0, users.count())

        # Некорректная модель
        self.assertRaises(TableModelException, users.insert, dbms_fw.get_new_tag_instance())
        self.assertEqual(0, users.count())
        # Список некорректных моделей
        self.assertRaises(TableModelException, users.insert, [dbms_fw.get_new_tag_instance()])
        self.assertEqual(0, users.count())
        # Список корректная модель + некорректная модель
        user = dbms_fw.get_new_user_instance()
        user.name = "Andrey"
        user.age = 81
        self.assertRaises(TableModelException, users.insert, [user, dbms_fw.get_new_tag_instance()])
        self.assertEqual(0, users.count())

        # Корректная модель
        users.insert(user)
        self.assertEqual(1, users.count())
        # Список корректных моделей
        user2 = dbms_fw.get_new_user_instance()
        user2.name = "Andrey"
        user3 = dbms_fw.get_new_user_instance()
        user3.name = "Alexey"
        users.insert([user2, user3])
        self.assertEqual(3, users.count())

        # Ну и проверим отказ в добавлении корректных с точки зрения формата моделей,
        # но нарушающих валидацию бизнес логики
        # В классе User поле count не может быть равно 42
        user4 = dbms_fw.get_new_user_instance()
        user4.name = "Ivan"
        user4.age = 42
        self.assertRaises(Exception, users.insert, user4)
        self.assertEqual(3, users.count())
        # И в списке:
        self.assertRaises(Exception, users.insert, [user4])
        self.assertEqual(3, users.count())

    @for_all_dbms
    def test_dbms_exceptions(self, dbms_fw):
        """ Проверим обработку исключений, генерируемых адаптерами. Они должны заменяться на исключения mapex """

        ##################################### DublicateRecord ###################################################
        users = dbms_fw.get_new_users_collection_instance()
        self.assertEqual(0, users.count())
        uid = users.insert({"name": "first"})
        self.assertEqual(1, users.count())

        from mapex.dbms.Adapters import DublicateRecord
        user2 = dbms_fw.get_new_user_instance({"uid": uid, "name": "second"})
        self.assertRaises(DublicateRecord, user2.save)



    @for_all_dbms
    def test_get_property_list(self, dbms_fw):
        """ Проверим способ получения свойств для объектов, хранящихся в модели """
        users = dbms_fw.get_new_users_collection_instance()
        users.insert([
            {"age": 99, "name": "valueForFieldName1"},
            {"age": 999, "name": "valueForFieldName2"},
            {"age": 9999, "name": "valueForFieldName3"}
        ])
        self.assertCountEqual(
            ["valueForFieldName1", "valueForFieldName2"],
            users.get_property_list("name", {"age": ("in", [99, 999])})
        )

        self.assertCountEqual(
            [
                {"name": "valueForFieldName1", "age": 99},
                {"name": "valueForFieldName2", "age": 999}
            ],
            users.get_properties_list(["name", "age"], {"age": ("in", [99, 999])})
        )

    @for_all_dbms
    def test_get_items(self, dbms_fw):
        """ Проверим метод получения моделей из коллекции """
        users = dbms_fw.get_new_users_collection_instance()
        item = users.get_item({"age": 1})
        self.assertIsNone(item)
        users.insert({"name": "FirstItem", "age": 1})
        users.insert({"name": "SecondItem", "age": 2})
        item = users.get_item({"age": 1})
        self.assertTrue(isinstance(item, dbms_fw.get_new_user_instance().__class__))
        self.assertEqual("FirstItem", item.name)
        self.assertRaises(TableModelException, users.get_item, {"age": ("gt", 0)})

        items = users.get_items({"age": ("in", [1, 2])}, {"order": ("uid", "asc")})
        self.assertTrue(isinstance(items, list))
        self.assertTrue(isinstance(items[0], dbms_fw.get_new_user_instance().__class__))
        self.assertEqual("FirstItem", items[0].name)
        self.assertEqual("SecondItem", items[1].name)

    @for_all_dbms
    def test_params(self, dbms_fw):
        """ Проверим сортировку и ограничение выборки """
        users = dbms_fw.get_new_users_collection_instance()
        users.insert({"name": "FirstItem", "age": 1})
        users.insert({"name": "SecondItem", "age": 2})
        res = users.get_items()
        self.assertEqual(2, len(res))

        res = users.get_items(params={"limit": 1})
        self.assertEqual(1, len(res))

        res = users.get_items(params={"limit": 1, "order": ("age", "ASC")})
        self.assertEqual(1, len(res))
        self.assertEqual("FirstItem", res[0].name)

        res = users.get_items(params={"limit": 1, "order": ("age", "DESC")})
        self.assertEqual(1, len(res))
        self.assertEqual("SecondItem", res[0].name)

    @for_all_dbms
    def test_query_with_links(self, dbms_fw):
        """ Проверим возможность запросов к коллекции с условиями, включающими обращения к полям типа Link """
        users = dbms_fw.get_new_users_collection_instance()
        accounts = dbms_fw.get_new_accounts_collection_instance()

        # Поле account маппера объявлено типом Link.
        # В него нельзя писать ничего кроме экземпляров класса dbms_fw.get_new_account_instance().__class__
        self.assertRaises(
            TableModelException,
            users.insert,
            {"name": "FirstUserName", "account": {"email": "aaaa", "phone": "911"}}
        )

        account1 = dbms_fw.get_new_account_instance()
        account1.phone = "112"
        account1.email = "first@email.ru"

        account2 = dbms_fw.get_new_account_instance()
        account2.phone = "911"
        account2.email = "second@email.ru"

        account3 = dbms_fw.get_new_account_instance()
        account3.phone = "007"
        account3.email = "third@email.ru"

        # Объекты заполнены данными, но не сохранены в бд:
        self.assertEqual(0, accounts.count())

        # Сохраняем первые два объекта:
        account1.save()
        account2.save()
        # Получаем две новые записи
        self.assertEqual(2, accounts.count())

        # Пользователей пока тоже нет:
        self.assertEqual(0, users.count())
        # Создадим двух пользователей и присвоим им созданные аккаунты, третьему из них присвоим объект аккаунта,
        # который еще не сохранен в БД, таким образом проверим, что он сохраняется при инсерте
        users.insert({"name": "FirstUser", "account": account1})
        users.insert({"name": "SecondUser", "account": account2})
        users.insert({"name": "ThirdUser", "account": account3})
        # И пользователей и аккаунтов стало 3
        self.assertEqual(3, accounts.count())
        self.assertEqual(3, users.count())

        # Теперь попробуем сделать поиск по коллекции users по данным аккаунта
        found_user = users.get_item({"account.email": "second@email.ru"})
        self.assertEqual(found_user.name, "SecondUser")
        self.assertEqual(found_user.account.phone, "911")

        # Теперь попробуем получить данные аккаунта из коллекции пользователей:
        self.assertCountEqual(
            ["first@email.ru", "second@email.ru", "third@email.ru"],
            list(users.get_property_list("account.email"))
        )

        self.assertCountEqual(
            ["second@email.ru"],
            list(users.get_property_list("account.email", {"name": "SecondUser"}))
        )

        self.assertCountEqual(
            [
                {"account.phone": '112', "account.email": "first@email.ru"},
                {"account.phone": '911', "account.email": "second@email.ru"},
                {"account.phone": '007', "account.email": "third@email.ru"}
            ],
            list(users.get_properties_list(["account.email", "account.phone"]))
        )
        self.assertCountEqual(
            [
                {"account.phone": '112', "name": "FirstUser"},
                {"account.phone": '911', "name": "SecondUser"},
                {"account.phone": '007', "name": "ThirdUser"}
            ],
            list(users.get_properties_list(["account.phone", "name"]))
        )
        self.assertCountEqual(
            [{"account.phone": '007', "name": "ThirdUser"}],
            list(users.get_properties_list(["account.phone", "name"], {"account.phone": "007"}))
        )

        # Теперь попробуем обновить данные одного из аккаунтов:
        found_user.account.email = "email@email.ru"
        found_user.account.save()
        # Новой записи не добавилось:
        self.assertEqual(3, accounts.count())
        # Зато данные аккаунта у второго пользователя изменились:
        found_user_new = users.get_item({"account.email": "email@email.ru"})
        self.assertEqual(found_user_new.name, "SecondUser")

        # Теперь попробуем создать новый аккаунт путем операции update на коллекции users:
        account_last = dbms_fw.get_new_account_instance()
        account_last.email = "last@email.com"
        account_last.phone = "666"
        users.update({"account": account_last}, {"account.email": "email@email.ru"})
        # Теперь у нас 4 аккаунта, но пользователей, разумеется, по-прежнему 3:
        self.assertEqual(4, accounts.count())
        self.assertEqual(3, users.count())

        # Теперь попробуем удалить запись по данным из аккаунта:
        users.delete({"account.phone": "666"})
        self.assertEqual(2, users.count())
        self.assertEqual(1, users.count({"account.email": "first@email.ru"}))
        # Коллекция аккаунтов не затрагивается:
        self.assertEqual(4, accounts.count())

    @for_all_dbms
    def test_query_with_lists(self, dbms_fw):
        """ Проверим возможность запросов к коллекции с условиями, включающими обращения к полям типа List """
        users = dbms_fw.get_new_users_collection_instance()
        tags = dbms_fw.get_new_tags_collection_instance()

         # Поле tags маппера объявлено типом List.
         # В него нельзя писать ничего кроме списков экземпляров класса dbms_fw.get_new_tag_instance().__class__
        self.assertRaises(
            TableModelException,
            users.insert,
            {"name": "FirstUserName", "tags": {"tagName": "someName"}}
        )
        self.assertRaises(
            TableModelException,
            users.insert,
            {"name": "FirstUserName", "tags": [{"tagName": "someName"}]}
        )
        self.assertRaises(
            TableModelException,
            users.insert,
            {"name": "FirstUserName", "tags": [dbms_fw.get_new_tag_instance(), {"tagName": "someName"}]}
        )

        # Создадим два тега, не сохраняя их
        tag1 = dbms_fw.get_new_tag_instance()
        tag1.name = "FirstTag"
        tag1.weight = 12

        tag2 = dbms_fw.get_new_tag_instance()
        tag2.name = "SecondTag"
        tag2.weight = 91

        self.assertEqual(0, tags.count())

        # Создадим первого пользователя, у которого есть оба тега
        first_user_id = users.insert({"name": "FirstUser", "tags": [tag1, tag2]})
        self.assertEqual(1, users.count())
        self.assertEqual(2, tags.count())

        # Создадим второго пользователя, у которого только один тег (из вух имеющихся)
        second_user_id = users.insert({"name": "SecondUser", "tags": [tag2]})
        self.assertEqual(2, users.count())
        self.assertEqual(2, tags.count())

        # Создадим третий тег, и сохраним его в БД
        tag3 = dbms_fw.get_new_tag_instance()
        tag3.name = "ThirdTag"
        tag3.weight = 64
        tag3.save()

        self.assertEqual(3, tags.count())

        # Получим объект второго пользователя и добавим ему созданный тег (чтобы было в сумме два)
        second_user = users.get_item({"uid": second_user_id})
        second_user.tags.append(tag3)
        second_user.save()
        self.assertEqual(2, users.count())
        self.assertEqual(3, tags.count())

        # Теперь с помощью операции уровня коллекции изменим список тегов у первого пользователя:
        users.update({"tags": [tag1, tag3]}, {"uid": first_user_id})
        self.assertEqual(2, users.count())
        self.assertEqual(3, tags.count())
        first_user = users.get_item({"uid": first_user_id})
        self.assertCountEqual(["FirstTag", "ThirdTag"], [tag.name for tag in first_user.tags])

        # Проверим можно ли очистить список
        # Заодно проверим работает ли update во множественном режиме:
        users.update({"tags": []})
        first_user = users.get_item({"uid": first_user_id})
        second_user = users.get_item({"uid": second_user_id})
        self.assertEqual([], first_user.tags)
        self.assertEqual([], second_user.tags)

        # Теперь вернем пользователям их теги и попробуем сделать пару выборок по данным тегов:
        first_user.tags = [tag1, tag3]
        first_user.save()
        second_user.tags = [tag2]
        second_user.save()

        res = users.get_items({"tags.name": ("in", ["FirstTag", "ThirdTag"])})
        self.assertEqual(1, len(res))
        self.assertEqual("FirstUser", res[0].name)

        res = users.get_items({"tags.weight": ("gt", 90)})
        self.assertEqual(1, len(res))
        self.assertEqual("SecondUser", res[0].name)

        res = users.get_items({"tags.name": ("in", ["FirstTag", "SecondTag"])})
        self.assertEqual(2, len(res))
        self.assertCountEqual(["FirstUser", "SecondUser"], [res[0].name, res[1].name])

        # join, основанный только на полях сортировки:
        res = list(users.get_items(params={"order": ("tags.name", "ASC")}))
        self.assertCountEqual(["FirstUser", "SecondUser"], [res[0].name, res[1].name])

        res = users.get_property_list("name", {"tags.name": ("in", ["FirstTag", "SecondTag"])})
        self.assertCountEqual(['FirstUser', 'SecondUser'], list(res))

        res = users.get_property_list("tags.weight", {"name": ("in", ["FirstUser", "SecondUser"])})
        self.assertCountEqual([12, 64, 91], list(res))
        res = users.get_property_list("tags.weight", {"name": "SecondUser"})
        self.assertCountEqual([91], list(res))

        # Попробуем выполнить запрос затрагивающий две присоединенные таблицы
        # Назначим однмоу из пользователей аккаунт:
        first_user.account = dbms_fw.get_new_account_instance()
        first_user.account.email = "email@email.com"
        first_user.account.save()
        first_user.save()
        res = list(users.get_property_list("account.email", {"tags.name": ("in", ["FirstTag", "SecondTag"])}))
        self.assertEqual(["email@email.com"], res)

        res = list(users.get_properties_list(
            ["tags.weight", "tags.name"], {"tags.name": ("in", ["FirstTag", "SecondTag"])}, {"order": ("uid", "ASC")})
        )
        self.assertEqual(
            [{'tags.name': 'FirstTag', 'tags.weight': 12}, {'tags.name': 'SecondTag', 'tags.weight': 91}],
            res
        )

        res = list(
            users.get_properties_list(
                ["tags.weight", "tags.name"], {"account.email": "email@email.com"}, {"order": ("tags.name", "ASC")}
            )
        )
        self.assertEqual(
            [{'tags.name': 'FirstTag', 'tags.weight': 12}, {'tags.name': 'ThirdTag', 'tags.weight': 64}],
            res
        )

        # Теперь попробуем удалить запись по данным о тегах:
        # Сперва убедимся в корректности всех трех таблиц:
        users_tags_collection = dbms_fw.get_new_userstags_collection_instance()
        self.assertEqual(2, users.count())                  # 2 пользователя
        self.assertEqual(3, tags.count())                   # 3 тега
        if users_tags_collection:                               # Для тех, кто реализует списки через таблицы отношений:
            self.assertEqual(3, users_tags_collection.count())  # 3 записи отношений
        # Удаляем:
        users.delete({"tags.name": "SecondTag"})            # Должен удалиться только один пользователь SecondUser
        # Проверяем результат:
        self.assertEqual(1, users.count())                  # Запись о пользователе удалена
        self.assertEqual(3, tags.count())                   # Коллекция тегов не затронута
        if users_tags_collection:                               # Для тех, кто реализует списки через таблицы отношений:
            self.assertEqual(2, users_tags_collection.count())  # 2 записи отношений - одна удалена с пользователем

    @for_all_dbms
    def test_query_with_reversed_links(self, dbms_fw):
        """ Проверим тим связи, когда в основном маппера нет ссылки на другую таблицу,
        но у какой-либо записи в другой таблице есть ссылка на запись в основной таблице
        """
        users = dbms_fw.get_new_users_collection_instance()
        profiles = dbms_fw.get_new_profiles_collection_instance()

        # Поле profile маппера объявлено типом ReversedLink.
        # В него нельзя писать ничего кроме экземпляров класса Profile
        self.assertRaises(
            TableModelException,
            users.insert,
            {"name": "FirstUserName", "profile": {"tagName": "someName"}}
        )
        self.assertRaises(
            TableModelException,
            users.insert,
            {"name": "FirstUserName", "profile": [{"tagName": "someName"}]}
        )

        # Создадим два статуса, не сохраняя их
        profile1 = dbms_fw.get_new_profile_instance()
        profile1.avatar = "FirstAvatar"
        profile1.likes = 12

        profile2 = dbms_fw.get_new_profile_instance()
        profile2.avatar = "SecondAvatar"
        profile2.likes = 91

        self.assertEqual(0, profiles.count())
        first_user_id = users.insert({"name": "FirstUser", "profile": profile1})
        second_user_id = users.insert({"name": "SecondUser", "profile": profile2})

        self.assertIsNotNone(profile1.id)
        self.assertIsNotNone(profile2.id)
        self.assertEqual(users.get_item({"uid": first_user_id}), profile1.user)
        self.assertEqual(users.get_item({"uid": second_user_id}), profile2.user)
        self.assertEqual(2, users.count())
        self.assertEqual(2, profiles.count())

        first_user = users.get_item({"uid": first_user_id})
        second_user = users.get_item({"uid": second_user_id})
        self.assertEqual(profile1, first_user.profile)
        self.assertEqual(profile2, second_user.profile)

        first_user.profile = None
        first_user.save()
        self.assertEqual(None, first_user.profile)
        self.assertEqual(profile2, second_user.profile)

        first_user = users.get_item({"uid": first_user_id})
        self.assertEqual(None, first_user.profile)

        # Вернем профиль первому пользователю:
        first_user.profile = profile1
        first_user.save()

        res = list(
            users.get_property_list(
                "profile.avatar",
                {"name": ("in", ["FirstUser", "SecondUser"])}, {"order": ("uid", "ASC")}
            )
        )
        self.assertEqual(['FirstAvatar', 'SecondAvatar'], res)

        res = list(
            users.get_property_list("name", {"profile.avatar": "SecondAvatar"}, {"order": ("uid", "ASC")})
        )
        self.assertEqual(['SecondUser'], res)

        res = list(
            users.get_property_list(
                "name",
                {"profile.avatar": ("in", ["FirstAvatar", "SecondAvatar"])}, {"order": ("uid", "ASC")}
            )
        )
        self.assertEqual(['FirstUser', 'SecondUser'], res)

        res = list(
            users.get_properties_list(
                ["profile.likes", "profile.avatar"],
                {"profile.avatar": ("in", ["FirstAvatar", "SecondAvatar"])},
                {"order": ("uid", "ASC")}
            )
        )
        self.assertEqual([{'profile.avatar': 'FirstAvatar', 'profile.likes': 12},
                          {'profile.avatar': 'SecondAvatar', 'profile.likes': 91}],
                         res)

        res = list(
            users.get_properties_list(
                ["profile.likes", "profile.avatar"], {"name": ("in", ["SecondUser"])}, {"order": ("uid", "ASC")}
            )
        )
        self.assertEqual(
            [{'profile.avatar': 'SecondAvatar', 'profile.likes': 91}],
            res
        )

        res = list(
            users.get_properties_list(
                ["profile.avatar", "name"],
                {"name": ("in", ["SecondUser"])},
                {"order": ("uid", "ASC")}
            )
        )
        self.assertEqual([{'profile.avatar': 'SecondAvatar', 'name': "SecondUser"}], res)

        # Теперь попробуем удалить запись по данным о статусах:
        # Сперва убедимся в корректности всех трех таблиц:
        self.assertEqual(2, users.count())                                  # 2 пользователя
        self.assertEqual(2, profiles.count())                               # 2 профиля
        self.assertEqual(1, profiles.count({"user.name": "SecondUser"}))    # 1 из 2 принадлежит SecondUser
        self.assertEqual(2, profiles.count({"user": ("exists", True)}))     # У всех записей указан пользователь
        # Удаляем SecondUser:
        users.delete({"profile.avatar": "SecondAvatar"})                   # Должен удалиться один юзер - SecondUser
        # Проверяем результат:
        self.assertEqual(1, users.count())                                  # Запись о пользователе удалена
        self.assertEqual(2, profiles.count())                               # Кол-во профилей не изменилось
        self.assertEqual(1, profiles.count({"user": ("exists", True)}))     # Но у одной из записей не указан юзер

    @for_all_dbms
    def test_query_with_reversed_lists(self, dbms_fw):
        """ Проверим возможность работы с привязанным списком объектов, реализованным без таблицы отношений """
        users = dbms_fw.get_new_users_collection_instance()
        statuses = dbms_fw.get_new_statuses_collection_instance()

        # Поле statuses маппера объявлено типом List.
        # В него нельзя писать ничего кроме списков экземпляров класса Status
        self.assertRaises(
            TableModelException,
            users.insert,
            {"name": "FirstUserName", "statuses": {"tagName": "someName"}}
        )
        self.assertRaises(
            TableModelException,
            users.insert,
            {"name": "FirstUserName", "statuses": [{"tagName": "someName"}]}
        )
        self.assertRaises(
            TableModelException,
            users.insert,
            {"name": "FirstUserName", "statuses": [dbms_fw.get_new_status_instance(), {"tagName": "someName"}]}
        )

        # Создадим два статуса, не сохраняя их
        status1 = dbms_fw.get_new_status_instance()
        status1.name = "FirstStatus"
        status1.weight = 12

        status2 = dbms_fw.get_new_status_instance()
        status2.name = "SecondStatus"
        status2.weight = 91

        self.assertEqual(0, statuses.count())

        # Создадим первого пользователя, у которого есть оба статуса
        first_user_id = users.insert({"name": "FirstUser", "statuses": [status1, status2]})

        self.assertIsNotNone(status1.id)
        self.assertIsNotNone(status2.id)
        self.assertEqual(users.get_item({"uid": first_user_id}), status1.user)
        self.assertEqual(users.get_item({"uid": first_user_id}), status2.user)
        self.assertEqual(1, users.count())
        self.assertEqual(2, statuses.count())

        # Создадим второго пользователя, у которого только один статус (из двух имеющихся)
        second_user_id = users.insert({"name": "SecondUser", "statuses": [status2]})

        self.assertIsNotNone(status1.id)
        self.assertIsNotNone(status2.id)
        self.assertEqual(users.get_item({"uid": first_user_id}), status1.user)
        self.assertCountEqual(users.get_item({"uid": first_user_id}).statuses, [status1])
        self.assertEqual(users.get_item({"uid": second_user_id}), status2.user)
        self.assertCountEqual(users.get_item({"uid": second_user_id}).statuses, [status2])
        self.assertEqual(2, users.count())
        self.assertEqual(2, statuses.count())

        # Создадим третий тег, и сохраним его в БД
        status3 = dbms_fw.get_new_status_instance()
        status3.name = "ThirdStatus"
        status3.weight = 64
        status3.save()
        self.assertEqual(3, statuses.count())

        # Создадим третьего пользователя, у которого только один статус (из имеющихся, уже сохраненных в базе)
        third_user_id = users.insert({"name": "ThirdUser", "statuses": [status3]})
        self.assertEqual(users.get_item({"uid": third_user_id}), status3.user)
        self.assertEqual(users.get_item({"uid": third_user_id}).statuses, [status3])
        self.assertEqual(3, users.count())
        self.assertEqual(3, statuses.count())

        # Попробуем поменять наборы статусов налету
        second_user = users.get_item({"uid": second_user_id})
        third_user = users.get_item({"uid": third_user_id})

        self.assertEqual([status2], second_user.statuses)
        self.assertEqual([status3], third_user.statuses)
        tmp = list(third_user.statuses)
        third_user.statuses = list(second_user.statuses)
        second_user.statuses = tmp
        third_user.save()
        second_user.save()
        second_user = users.get_item({"uid": second_user_id})
        third_user = users.get_item({"uid": third_user_id})
        self.assertEqual(3, statuses.count())
        self.assertEqual([status3], second_user.statuses)
        self.assertEqual([status2], third_user.statuses)

        # Добавим второй статус второму пользователю
        second_user.statuses.append(status2)
        second_user.save()
        self.assertCountEqual([status2, status3], second_user.statuses)
        second_user = users.get_item({"uid": second_user_id})
        self.assertCountEqual([status2, status3], second_user.statuses)

        self.assertCountEqual([status2], third_user.statuses)   # Ибо эта модель еще не знает, что ее изменили
        third_user = users.get_item({"uid": third_user_id})
        self.assertCountEqual([], third_user.statuses)          # Теперь все актуально...
        self.assertEqual(3, users.count())
        self.assertEqual(3, statuses.count())

        # Теперь с помощью операции уровня коллекции изменим список тегов у первого пользователя:
        users.update({"statuses": [status1, status3]}, {"uid": third_user_id})
        self.assertEqual(3, users.count())
        self.assertEqual(3, statuses.count())
        third_user = users.get_item({"uid": third_user_id})
        second_user = users.get_item({"uid": second_user_id})
        self.assertCountEqual([status1, status3], third_user.statuses)
        self.assertCountEqual([status2], second_user.statuses)

        # Проверим можно ли очистить список
        # Заодно проверим работает ли update во множественном режиме:
        users.update({"statuses": []})
        self.assertEqual(3, users.count())
        self.assertEqual(3, statuses.count())
        first_user = users.get_item({"uid": first_user_id})
        second_user = users.get_item({"uid": second_user_id})
        third_user = users.get_item({"uid": third_user_id})
        self.assertEqual([], first_user.statuses)
        self.assertEqual([], third_user.statuses)
        self.assertEqual([], second_user.statuses)

        # Теперь вернем пользователям их статусы и попробуем сделать пару выборок по данным статусов:
        first_user.statuses = [status1, status3]
        first_user.save()
        second_user.statuses = [status2]
        second_user.save()
        res = users.get_items({"statuses.name": ("in", ["FirstStatus", "ThirdStatus"])})
        self.assertEqual(1, len(res))
        self.assertEqual("FirstUser", res[0].name)

        res = users.get_items({"statuses.weight": ("gt", 90)})
        self.assertEqual(1, len(res))
        self.assertEqual("SecondUser", res[0].name)

        res = users.get_items({"statuses.name": ("in", ["FirstStatus", "SecondStatus"])})
        self.assertEqual(2, len(res))
        self.assertCountEqual(["FirstUser", "SecondUser"], [res[0].name, res[1].name])

        res = users.get_property_list("name", {"statuses.name": ("in", ["FirstStatus", "SecondStatus"])})
        self.assertCountEqual(['FirstUser', 'SecondUser'], list(res))

        res = users.get_property_list("statuses.weight", {"name": ("in", ["FirstUser", "SecondUser"])})
        self.assertCountEqual([12, 64, 91], list(res))
        res = users.get_property_list("statuses.weight", {"name": "SecondUser"})
        self.assertCountEqual([91], list(res))

        res = list(users.get_properties_list(
            ["statuses.weight", "statuses.name"],
            {"statuses.name": ("in", ["FirstStatus", "SecondStatus"])}, {"order": ("uid", "ASC")})
        )
        self.assertEqual([{'statuses.name': 'FirstStatus', 'statuses.weight': 12},
                          {'statuses.name': 'SecondStatus', 'statuses.weight': 91}], res)

        # Теперь попробуем удалить запись по данным о статусах:
        # Сперва убедимся в корректности всех трех таблиц:
        self.assertEqual(3, users.count())                                  # 3 пользователя
        self.assertEqual(3, statuses.count())                               # 3 статуса
        self.assertEqual(1, statuses.count({"user.name": "SecondUser"}))    # 1 из 3 принадлежит SecondUser
        self.assertEqual(3, statuses.count({"user": ("exists", True)}))     # У всех записей указан пользователь
        # Удаляем SecondUser:
        users.delete({"statuses.weight": 91})                               # Должен удалиться только SecondUser
        # Проверяем результат:
        self.assertEqual(2, users.count())                                  # Запись о пользователе удалена
        self.assertEqual(3, statuses.count())                               # Кол-во статусов не изменилось
        self.assertEqual(2, statuses.count({"user": ("exists", True)}))     # Но у одной из записей не указан юзер

    @for_all_dbms
    def test_query_with_embeded_links(self, dbms_fw):
        """ Вложенный документ """
        users = dbms_fw.get_new_users_collection_instance()
        passports = dbms_fw.get_new_passports_collection_instance()

        # Поле passport маппера объявлено типом EmbeddedLink.
        # В него нельзя писать ничего кроме экземпляров класса Passport
        self.assertRaises(
            TableModelException,
            users.insert,
            {"name": "FirstUserName", "passport": {"series": 4216, "number": 2578758}}
        )

        # Создадим три паспорта, не сохраняя их
        passport0 = dbms_fw.get_new_passport_instance()
        passport0.series = 3218
        passport0.number = 668856

        passport1 = dbms_fw.get_new_passport_instance()
        passport1.series = 4215
        passport1.number = 123456

        passport2 = dbms_fw.get_new_passport_instance()
        passport2.series = 1432
        passport2.number = 789463

        # Первый сохраним самостоятельно:
        if passports:
            passport0.save()
            self.assertIsNotNone(passport0.id)
        else:
            self.assertRaises(TableModelException, passport0.save)

        # Два других прикрепим к пользователям:
        first_user_id = users.insert({"name": "FirstUser", "passport": passport1})
        second_user_id = users.insert({"name": "SecondUser", "passport": passport2})

        self.assertEqual(users.get_item({"uid": first_user_id}).passport, passport1)
        self.assertEqual(users.get_item({"uid": second_user_id}).passport, passport2)
        self.assertEqual(2, users.count())
        if passports:
            self.assertEqual(3, passports.count())
            self.assertEqual(2, passports.count({"user": ("exists", True)}))

        first_user = users.get_item({"uid": first_user_id})
        second_user = users.get_item({"uid": second_user_id})
        self.assertEqual(passport1, first_user.passport)
        self.assertEqual(passport2, second_user.passport)

        first_user.passport = None
        first_user.save()
        self.assertEqual(None, first_user.passport)
        self.assertEqual(passport2, second_user.passport)

        first_user = users.get_item({"uid": first_user_id})
        self.assertEqual(None, first_user.passport)

        # Вернем паспорт первому пользователю:
        first_user.passport = passport1
        first_user.save()

        # Выборка пользователей по номеру пасспорта
        found_user = users.get_item({"passport.number": 789463})
        self.assertEqual(second_user.uid, found_user.uid)

        res = list(
            users.get_property_list(
                "passport.number",
                {"name": ("in", ["FirstUser", "SecondUser"])},
                {"order": ("uid", "ASC")}
            )
        )
        self.assertEqual([123456, 789463], res)

        res = list(users.get_property_list("name", {"passport.number": 789463}, {"order": ("uid", "ASC")}))
        self.assertEqual(['SecondUser'], res)

        res = list(
            users.get_property_list(
                "name",
                {"passport.number": ("in", [123456, 789463])},
                {"order": ("uid", "ASC")})
        )
        self.assertEqual(['FirstUser', 'SecondUser'], res)

        res = list(
            users.get_properties_list(
                ["passport.series", "passport.number"],
                {"passport.number": ("in", [123456, 789463])},
                {"order": ("uid", "ASC")}
            )
        )
        self.assertEqual([{'passport.number': 123456, 'passport.series': 4215},
                          {'passport.number': 789463, 'passport.series': 1432}], res)

        res = list(
            users.get_properties_list(
                ["passport.series", "passport.number"],
                {"name": ("in", ["SecondUser"])},
                {"order": ("uid", "ASC")}
            )
        )
        self.assertEqual([{'passport.number': 789463, 'passport.series': 1432}], res)

        res = list(
            users.get_properties_list(
                ["passport.number", "name"],
                {"name": ("in", ["SecondUser"])},
                {"order": ("uid", "ASC")}
            )
        )
        self.assertEqual([{'passport.number': 789463, 'name': "SecondUser"}], res)

        # Теперь попробуем удалить запись по данным о паспортах:
        # Сперва убедимся в корректности всех трех таблиц:
        self.assertEqual(2, users.count())                                       # 2 пользователя
        if passports:
            self.assertEqual(3, passports.count())                               # 3 паспорта
            self.assertEqual(1, passports.count({"user.name": "SecondUser"}))    # 1 из 2 принадлежит SecondUser
            self.assertEqual(2, passports.count({"user": ("exists", True)}))     # У 2 записей указан пользователь

        # Удаляем SecondUser:
        users.delete({"passport.number": 789463})                                # Должен удалиться один SecondUser
        # Проверяем результат:
        self.assertEqual(1, users.count())                                       # Запись о пользователе удалена
        if passports:
            self.assertEqual(2, passports.count())                               # На один паспорт меньше
            self.assertEqual(1, passports.count({"user": ("exists", True)}))     # Только у одной из записей указан юзер

    @for_all_dbms
    def test_query_with_embedded_lists(self, dbms_fw):
        """ Проверим возможность работы с встроенным списком объектов """
        users = dbms_fw.get_new_users_collection_instance()
        documents = dbms_fw.get_new_documents_collection_instance()

        # Поле documents маппера объявлено типом EmbeddedList.
        # В него нельзя писать ничего кроме списков экземпляров класса Document
        self.assertRaises(
            TableModelException,
            users.insert,
            {"name": "FirstUserName", "documents": {"series": 1234, "number": 123456}}
        )
        self.assertRaises(
            TableModelException,
            users.insert,
            {"name": "FirstUserName", "documents": [{"series": 1234, "number": 123456}]}
        )
        self.assertRaises(
            TableModelException,
            users.insert,
            {
                "name": "FirstUserName",
                "documents": [
                    dbms_fw.get_new_document_instance(),
                    {"series": 1234, "number": 123456}
                ]
            }
        )

        # Создадим два паспорта, не сохраняя их
        document1 = dbms_fw.get_new_document_instance()
        document1.series = 4212
        document1.number = 126458

        document2 = dbms_fw.get_new_document_instance()
        document2.series = 7859
        document2.number = 911456

        # Создадим первого пользователя, у которого есть оба паспорта
        first_user_id = users.insert({"name": "FirstUser", "documents": [document1, document2]})
        self.assertCountEqual(
            [d.number for d in users.get_item({"uid": first_user_id}).documents],
            [document1.number, document2.number]
        )

        self.assertEqual(1, users.count())
        if documents:
            self.assertEqual(2, documents.count())

        # Создадим второго пользователя, у которого только один паспорт (из двух имеющихся)
        second_user_id = users.insert({"name": "SecondUser", "documents": [document2]})

        self.assertCountEqual(
            [d.number for d in users.get_item({"uid": first_user_id}).documents],
            [document1.number, document2.number]
        )
        self.assertCountEqual(
            [d.number for d in users.get_item({"uid": second_user_id}).documents],
            [document2.number]
        )
        self.assertEqual(2, users.count())
        if documents:
            self.assertEqual(3, documents.count())

        # Создадим третий паспорт
        document3 = dbms_fw.get_new_document_instance()
        document3.series = 4596
        document3.number = 642458

        # Создадим третьего пользователя, у которого только один паспорт
        third_user_id = users.insert({"name": "ThirdUser", "documents": [document3]})
        self.assertCountEqual(
            [d.number for d in users.get_item({"uid": third_user_id}).documents],
            [document3.number]
        )
        self.assertEqual(3, users.count())
        if documents:
            self.assertEqual(4, documents.count())

        # Попробуем поменять наборы статусов налету
        second_user = users.get_item({"uid": second_user_id})
        third_user = users.get_item({"uid": third_user_id})

        self.assertEqual([document2.number], [d.number for d in second_user.documents])
        self.assertEqual([document3.number], [d.number for d in third_user.documents])
        tmp = third_user.documents
        third_user.documents = list(second_user.documents)
        second_user.documents = list(tmp)
        third_user.save()
        second_user.save()
        if documents:
            self.assertEqual(4, documents.count())
        self.assertEqual([document3.number], [d.number for d in second_user.documents])
        self.assertEqual([document2.number], [d.number for d in third_user.documents])
        second_user = users.get_item({"uid": second_user_id})
        third_user = users.get_item({"uid": third_user_id})
        self.assertEqual([document3.number], [d.number for d in second_user.documents])
        self.assertEqual([document2.number], [d.number for d in third_user.documents])

        # Добавим второй статус второму пользователю
        second_user.documents.append(document2)
        second_user.save()
        self.assertCountEqual([document2.number, document3.number], [d.number for d in second_user.documents])
        second_user = users.get_item({"uid": second_user_id})
        self.assertCountEqual([document2.number, document3.number], [d.number for d in second_user.documents])

        self.assertCountEqual([document2.number], [d.number for d in third_user.documents])
        third_user = users.get_item({"uid": third_user_id})
        self.assertCountEqual([document2.number], [d.number for d in third_user.documents])
        self.assertEqual(3, users.count())
        if documents:
            self.assertEqual(5, documents.count())

        # Теперь с помощью операции уровня коллекции изменим список тегов у первого пользователя:
        users.update({"documents": [document1, document3]}, {"uid": third_user_id})
        self.assertEqual(3, users.count())
        if documents:
            self.assertEqual(6, documents.count())
        third_user = users.get_item({"uid": third_user_id})
        second_user = users.get_item({"uid": second_user_id})
        self.assertCountEqual([document1.number, document3.number], [d.number for d in third_user.documents])
        self.assertCountEqual([document2.number, document3.number], [d.number for d in second_user.documents])

        # Проверим можно ли очистить список
        # Заодно проверим работает ли update во множественном режиме:
        users.update({"documents": []})
        self.assertEqual(3, users.count())
        if documents:
            self.assertEqual(0, documents.count())
        first_user = users.get_item({"uid": first_user_id})
        second_user = users.get_item({"uid": second_user_id})
        third_user = users.get_item({"uid": third_user_id})
        self.assertEqual([], first_user.documents)
        self.assertEqual([], third_user.documents)
        self.assertEqual([], second_user.documents)

        # Теперь вернем пользователям их статусы и попробуем сделать пару выборок по данным статусов:
        first_user.documents = [document1, document3]
        first_user.save()
        second_user.documents = [document2]
        second_user.save()
        res = users.get_items({"documents.series": ("in", [4212, 4596])})
        self.assertEqual(1, len(res))
        self.assertEqual("FirstUser", res[0].name)

        res = users.get_property_list("name", {"documents.series": ("in", [4212, 7859])})
        self.assertCountEqual(['FirstUser', 'SecondUser'], list(res))

        res = users.get_property_list("documents.number", {"name": ("in", ["FirstUser", "SecondUser"])})
        self.assertCountEqual([126458, 642458, 911456], list(res))
        res = users.get_property_list("documents.series", {"name": "SecondUser"})
        self.assertCountEqual([7859], list(res))

        # Не проходит для mongodb, так как нужно отсекать исходные условия... но не ясно как это делать...
        res = list(
            users.get_properties_list(
                ["documents.series", "documents.number"],
                {"documents.series": ("in", [4212, 7859])},
                {"order": ("uid", "ASC")}
            )
        )
        self.assertEqual([{'documents.number': 126458, 'documents.series': 4212},
                          {'documents.number': 911456, 'documents.series': 7859}], res)

        res = list(
            users.get_properties_list(
                ["documents.series", "documents.number"],
                {"name": ("in", ["SecondUser"])},
                {"order": ("uid", "ASC")}
            )
        )
        self.assertEqual([{'documents.number': 911456, 'documents.series': 7859}], res)

        # Теперь попробуем удалить запись по данным о статусах:
        # Сперва убедимся в корректности всех трех таблиц:
        self.assertEqual(3, users.count())                                   # 3 пользователя
        if documents:
            self.assertEqual(3, documents.count())                               # 3 документа
            self.assertEqual(1, documents.count({"user.name": "SecondUser"}))    # 1 из 3 принадлежит SecondUser
            self.assertEqual(3, documents.count({"user": ("exists", True)}))     # У всех записей указан пользователь
        # Удаляем SecondUser:
        users.delete({"documents.series": 7859})                             # Должен удалиться только SecondUser
        # Проверяем результат:
        self.assertEqual(2, users.count())                                   # Запись о пользователе удалена
        if documents:
            self.assertEqual(2, documents.count())                           # Стало на один паспорт меньше

    @for_all_dbms
    def test_working_with_table_without_primary_crud(self, dbms_fw):
        """ Проверим основные особенности работы с таблицами без первичного ключа """
        collection_without_primary = dbms_fw.get_new_noprimary_collection_instance()
        self.assertEqual(0, collection_without_primary.count())

        item = dbms_fw.get_new_noprimary_instance()
        item.name = "FirstName"
        item.value = 12
        self.assertRaises(TableModelException, item.save)
        self.assertEqual(0, collection_without_primary.count())

        collection_without_primary.insert(item)
        self.assertEqual(1, collection_without_primary.count({"name": "FirstName", "value": 12}))
        collection_without_primary.insert(item)
        self.assertEqual(2, collection_without_primary.count({"name": "FirstName", "value": 12}))
        collection_without_primary.insert({"name": "SecondName", "value": 31})
        self.assertEqual(2, collection_without_primary.count({"name": "FirstName", "value": 12}))
        self.assertEqual(1, collection_without_primary.count({"name": "SecondName", "value": 31}))
        self.assertEqual(3, collection_without_primary.count())

        collection_without_primary.update({"name": "NewName"}, {"name": "SecondName", "value": 31})
        self.assertEqual(2, collection_without_primary.count({"name": "FirstName", "value": 12}))
        self.assertEqual(0, collection_without_primary.count({"name": "SecondName", "value": 31}))
        self.assertEqual(1, collection_without_primary.count({"name": "NewName", "value": 31}))

        self.assertRaises(TableModelException, item.remove)
        collection_without_primary.delete({"name": "NewName", "value": 31})
        self.assertEqual(2, collection_without_primary.count({"name": "FirstName", "value": 12}))
        self.assertEqual(0, collection_without_primary.count({"name": "SecondName", "value": 31}))
        self.assertEqual(0, collection_without_primary.count({"name": "NewName", "value": 31}))

        collection_without_primary.insert({"name": "SecondName", "value": 31})
        self.assertEqual(2, collection_without_primary.count({"name": "FirstName", "value": 12}))
        self.assertEqual(1, collection_without_primary.count({"name": "SecondName", "value": 31}))

        self.assertRaises(TableModelException, collection_without_primary.get_item, {"name": "FirstName", "value": 12})
        second_item = collection_without_primary.get_item({"name": "SecondName", "value": 31})
        self.assertEqual(second_item.name, "SecondName")

    @for_all_dbms
    def test_working_with_defined_primary_in_mapper(self, dbms_fw):
        """
        Проверим возможность указания первичного ключа для таблиц,
        ксли он не указан в БД или в случае необходимости переопределения
        """
        collection_without_primary = dbms_fw.get_new_noprimary_collection_instance()
        collection_without_primary.mapper.set_primary("name")
        self.assertEqual(0, collection_without_primary.count())

        item = dbms_fw.get_new_noprimary_instance()
        item.name = "FirstName"
        item.value = 91
        ret = item.save()
        self.assertEqual("FirstName", ret)
        self.assertEqual(1, collection_without_primary.count())

        item.save()
        self.assertEqual(1, collection_without_primary.count({"value": 91}))
        item.value = 12
        item.save()
        self.assertEqual(0, collection_without_primary.count({"value": 91}))
        self.assertEqual(1, collection_without_primary.count({"value": 12}))

        collection_without_primary.insert(item)
        collection_without_primary.insert({"name": "SecondName", "value": 31})
        self.assertEqual(2, collection_without_primary.count({"name": "FirstName", "value": 12}))
        self.assertEqual(1, collection_without_primary.count({"name": "SecondName", "value": 31}))
        self.assertEqual(3, collection_without_primary.count())

        collection_without_primary.update({"name": "NewName"}, {"name": "SecondName", "value": 31})
        self.assertEqual(2, collection_without_primary.count({"name": "FirstName", "value": 12}))
        self.assertEqual(0, collection_without_primary.count({"name": "SecondName", "value": 31}))
        self.assertEqual(1, collection_without_primary.count({"name": "NewName", "value": 31}))

        self.assertRaises(TableModelException, collection_without_primary.get_item, {"name": "FirstName", "value": 12})

        new_value_unique = collection_without_primary.get_item({"name": "NewName", "value": 31})
        new_value_unique.remove()
        self.assertEqual(2, collection_without_primary.count({"name": "FirstName", "value": 12}))
        self.assertEqual(0, collection_without_primary.count({"name": "NewName", "value": 31}))

        item.remove()
        self.assertEqual(0, collection_without_primary.count({"name": "FirstName", "value": 12}))
        self.assertEqual(0, collection_without_primary.count())

        collection_without_primary.mapper.set_primary(None)

    @for_all_dbms
    def test_working_with_compound_primary_in_mapper(self, dbms_fw):
        """ Проверим возможность указания первичного ключа для таблиц в виде составного ключа """
        collection_without_primary = dbms_fw.get_new_noprimary_collection_instance()
        collection_without_primary.mapper.set_primary(["name", "value"])
        self.assertEqual(0, collection_without_primary.count())

        item = dbms_fw.get_new_noprimary_instance()
        item.name = "FirstName"
        item.value = 91
        ret = item.save()
        self.assertEqual({'name': 'FirstName', 'value': 91}, ret)
        self.assertEqual(1, collection_without_primary.count())

        item.save()
        self.assertEqual(1, collection_without_primary.count({"value": 91}))
        item.value = 12
        item.save()
        self.assertEqual(0, collection_without_primary.count({"value": 91}))
        self.assertEqual(1, collection_without_primary.count({"value": 12}))

        collection_without_primary.insert(item)
        collection_without_primary.insert({"name": "SecondName", "value": 31})
        self.assertEqual(2, collection_without_primary.count({"name": "FirstName", "value": 12}))
        self.assertEqual(1, collection_without_primary.count({"name": "SecondName", "value": 31}))
        self.assertEqual(3, collection_without_primary.count())

        collection_without_primary.update({"name": "NewName"}, {"name": "SecondName", "value": 31})
        self.assertEqual(2, collection_without_primary.count({"name": "FirstName", "value": 12}))
        self.assertEqual(0, collection_without_primary.count({"name": "SecondName", "value": 31}))
        self.assertEqual(1, collection_without_primary.count({"name": "NewName", "value": 31}))

        # Исключение, потому что get_item вынужден вернуть более одной записи
        self.assertRaises(
            TableModelException, collection_without_primary.get_item, {"name": "FirstName", "value": 12}
        )

        new_value_unique = collection_without_primary.get_item({"name": "NewName", "value": 31})
        new_value_unique.remove()
        self.assertEqual(2, collection_without_primary.count({"name": "FirstName", "value": 12}))
        self.assertEqual(0, collection_without_primary.count({"name": "NewName", "value": 31}))

        item.remove()
        self.assertEqual(0, collection_without_primary.count({"name": "FirstName", "value": 12}))
        self.assertEqual(0, collection_without_primary.count())

        collection_without_primary.mapper.set_primary(None)

    @for_all_dbms
    def test_working_with_table_without_primary_as_secondary_table(self, dbms_fw):
        """ Проверим основные особенности работы с таблицами без первичного ключа в качестве присоединенных таблиц """
        users = dbms_fw.get_new_users_collection_instance()
        collection_without_primary = dbms_fw.get_new_noprimary_collection_instance()

        # Изменим маппер коллекции users так, чтобы поле account смотрело на нашу таблицу, не имеющую первичного ключа
        # При поптыке сделать это получаем исключение, так как связать мапперы без primary key невозможно
        self.assertRaises(
            TableModelException,
            lambda: (
                users.mapper.set_field(
                    dbms_fw.get_link_field_type()(
                        users.mapper, "account",
                        db_field_name="AccountID", db_field_type=dbms_fw.get_foreign_key_field_type(),
                        joined_collection=dbms_fw.get_new_noprimary_collection_instance().__class__
                    )
                )
            )
        )

        # Теперь назначим первичный ключ для ущербной таблицы
        collection_without_primary.mapper.set_primary("value")
        # Теперь все нормально
        users.mapper.set_field(
            dbms_fw.get_link_field_type()(
                users.mapper, "account",
                db_field_name="AccountID", db_field_type=dbms_fw.get_foreign_key_field_type(),
                joined_collection=dbms_fw.get_new_noprimary_collection_instance().__class__
            )
        )
        # Так как в продакшене мапперы не будут конфигурироваться динамически,
        # а будут инициализироваться в конструкторе - такой проблемы не будет,
        # но здесь, в тестах, надо вызвать перестроение внутреннего представления маппера коллекции users,
        # чтобы маппер узнал о том, как он в действительности связан с ущербной таблицей
        # noinspection PyProtectedMember
        users.mapper._analyze_map()

        # Создадим запись в ущербной таблице
        no_primary_key_item1 = dbms_fw.get_new_noprimary_instance()
        no_primary_key_item1.name = "FirstItem"
        no_primary_key_item1.value = 42

        # Теперь вставка работает и мы можем убедиться в корректности привязки
        users.insert({"name": "FirstUser", "account": no_primary_key_item1})
        user = users.get_item({"account.name": "FirstItem"})
        self.assertEqual("FirstUser", user.name)

        # Возвращает все на свои места
        collection_without_primary.mapper.set_primary(None)
        users.mapper.set_field(
            dbms_fw.get_link_field_type()(
                users.mapper, "account",
                db_field_name="AccountID", db_field_type=dbms_fw.get_foreign_key_field_type(),
                joined_collection=dbms_fw.get_new_accounts_collection_instance().__class__
            )
        )

    @for_all_dbms
    def test_working_with_table_without_primary_as_secondary_table_joined_as_list(self, dbms_fw):
        """ Проверим особенности работы с таблицами без первичного ключа в качестве присоединенных м-к-м таблиц """
        users = dbms_fw.get_new_users_collection_instance()
        collection_without_primary = dbms_fw.get_new_noprimary_collection_instance()

        # Изменим маппер коллекции users так, чтобы поле account смотрело на нашу таблицу, не имеющую первичного ключа
        # Без установленного первичного ключа оно работать не будет:
        self.assertRaises(
            TableModelException,
            lambda: users.mapper.set_field(
                dbms_fw.get_list_field_type()(
                    mapper=users.mapper, mapper_field_name="statuses",
                    db_field_type=dbms_fw.get_foreign_key_field_type(),
                    joined_collection=dbms_fw.get_new_noprimary_collection_instance().__class__
                )
            )
        )

        # Поэтому назначим первичный ключ для ущербной таблицы:
        collection_without_primary.mapper.set_primary("value")
        users.mapper.set_field(
            dbms_fw.get_list_field_type()(
                mapper=users.mapper, mapper_field_name="statuses",
                db_field_type=dbms_fw.get_foreign_key_field_type(),
                joined_collection=dbms_fw.get_new_noprimary_collection_instance().__class__
            )
        )
        # noinspection PyProtectedMember
        users.mapper._analyze_map()

        # Создадим запись в ущербной таблице
        no_primary_key_item1 = dbms_fw.get_new_noprimary_instance()
        no_primary_key_item1.name = "FirstItem"
        no_primary_key_item1.value = 42

        no_primary_key_item2 = dbms_fw.get_new_noprimary_instance()
        no_primary_key_item2.name = "SecondItem"
        no_primary_key_item2.value = 21

        # Теперь вставка работает и мы можем убедиться в корректности привязки
        users.insert({"name": "FirstUser", "statuses": [no_primary_key_item1, no_primary_key_item2]})
        user1 = users.get_item({"statuses.name": "FirstItem"})
        user2 = users.get_item({"statuses.name": "SecondItem"})
        self.assertEqual("FirstUser", user1.name)
        self.assertEqual("FirstUser", user2.name)
        self.assertEqual(1, users.count())
        self.assertEqual(2, collection_without_primary.count())

        # Возвращает все на свои места
        collection_without_primary.mapper.set_primary(None)
        users.mapper.set_field(
            dbms_fw.get_list_field_type()(
                mapper=users.mapper, mapper_field_name="statuses",
                db_field_type=dbms_fw.get_foreign_key_field_type(),
                joined_collection=dbms_fw.get_new_statuses_collection_instance().__class__
            )
        )

    @for_all_dbms
    def test_working_with_table_without_primary_as_secondary_table_joined_as_list_with_compound_key(self, dbms_fw):
        """ Проверим особенности работы с таблицами без первичного ключа в качестве присоединенных м-к-м таблиц """
        users = dbms_fw.get_new_users_collection_instance()
        collection_without_primary = dbms_fw.get_new_noprimary_collection_instance()

        # Поэтому назначим первичный ключ для ущербной таблицы:
        collection_without_primary.mapper.set_primary(["name", "value"])
        users.mapper.set_field(
            dbms_fw.get_list_field_type()(
                mapper=users.mapper, mapper_field_name="statuses",
                db_field_type=dbms_fw.get_foreign_key_field_type(),
                joined_collection=dbms_fw.get_new_noprimary_collection_instance().__class__
            )
        )
        # noinspection PyProtectedMember
        users.mapper._analyze_map()

        # Создадим запись в ущербной таблице
        no_primary_key_item1 = dbms_fw.get_new_noprimary_instance()
        no_primary_key_item1.name = "FirstItem"
        no_primary_key_item1.value = 42

        no_primary_key_item2 = dbms_fw.get_new_noprimary_instance()
        no_primary_key_item2.name = "SecondItem"
        no_primary_key_item2.value = 21

        # Теперь вставка работает и мы можем убедиться в корректности привязки
        users.insert({"name": "FirstUser", "statuses": [no_primary_key_item1, no_primary_key_item2]})
        user1 = users.get_item({"statuses.name": "FirstItem"})
        user2 = users.get_item({"statuses.name": "SecondItem"})
        self.assertEqual("FirstUser", user1.name)
        self.assertEqual("FirstUser", user2.name)
        self.assertEqual(1, users.count())
        self.assertEqual(2, collection_without_primary.count())

        # Возвращает все на свои места
        collection_without_primary.mapper.set_primary(None)
        users.mapper.set_field(
            dbms_fw.get_list_field_type()(
                mapper=users.mapper, mapper_field_name="statuses",
                db_field_type=dbms_fw.get_foreign_key_field_type(),
                joined_collection=dbms_fw.get_new_statuses_collection_instance().__class__
            )
        )

    @for_all_dbms
    def test_performance_things_on_getting_items(self, dbms_fw):
        """ Проверим основные особенности получения элементов коллекции с точки зрения производительности """
        users = dbms_fw.get_new_users_collection_instance()
        tags = dbms_fw.get_new_tags_collection_instance()
        accounts = dbms_fw.get_new_accounts_collection_instance()

         # Создадим два тега
        tag1 = dbms_fw.get_new_tag_instance()
        tag1.name = "FirstTag"
        tag1.weight = 12
        tag1.save()

        tag2 = dbms_fw.get_new_tag_instance()
        tag2.name = "SecondTag"
        tag2.weight = 91
        tag2.save()

        tag3 = dbms_fw.get_new_tag_instance()
        tag3.name = "ThirdTag"
        tag3.weight = 42
        tag3.save()
        self.assertEqual(3, tags.count())

        account1 = dbms_fw.get_new_account_instance()
        account1.email = "first_email@sss.ru"
        account1.save()

        account2 = dbms_fw.get_new_account_instance()
        account2.email = "second_email@sss.ru"
        account2.save()

        account3 = dbms_fw.get_new_account_instance()
        account3.email = "third_email@sss.ru"
        account3.save()
        self.assertEqual(3, accounts.count())

        # Создадим пользователей
        users.insert({"name": "FirstUser", "account": account1, "tags": [tag1, tag2]})
        users.insert({"name": "SecondUser", "account": account2,  "tags": [tag2]})
        users.insert({"name": "ThirdUser", "account": account3,  "tags": [tag3]})
        self.assertEqual(3, users.count())

        # нам потребуется считать запросы к БД,
        # так как адаптер внутри всех мапперов используется один и тот же,
        # то подсчет можно делать с помощью следующей функции:
        #
        connection = users.mapper.db
        connection.start_logging()

        # Проверим, что подсчет ведется корректно хотя бы для простого случая
        initial_count = connection.count_queries()
        users.count()
        self.assertEqual(initial_count + 1, connection.count_queries())

        # Для теста будем получать все элементы с полным набором прилегающих данных и считать как тратятся запросы
        # Самый частоиспользуемый метод при разработке - "Минимальное кол-во запросов / Без экономии памяти"
        # Принцип такой: используем метод get_rows маппера,
        # одним запросом получаем всю инорфмацию из основной таблицы,
        # анализируем слинкованные объекты и собираем в память их ID,
        # а затем, еще за несколько запросов собираем в память соответствующие этим ID массивы данных, так чтобы
        # lazyLoading созданных объектов использовал этот массив вместо запросов к базе (что-то вроде кэша)
        # Кроме того, метод get_items должен принимать опциональный параметр lazyLoad=[fieldName1, fieldName2],
        # где могут быть указаны свойства объекта не подлежащие кэшированию в память,
        # а использующие стандартную отложенную инициализацию с помощью запроса в момент обращения к их свойствам

        # Расчетное кол-во запросов:
        # 3 (Основная выборка (3 элемента), Данные аккаунтов (3 элемента), Данные тегов (3 элемента)
        # Момент траты: по одному запросу на каждую сущность по мере запроса
        initial_count = connection.count_queries()
        count = initial_count
        count += dbms_fw.get_queries_amount("get_items")
        users_collection = users.get_items()
        self.assertEqual(count, connection.count_queries())
        # При этом получили три элемента, содержащие все данные, но прилинкованные данные пока ждут инициализации
        self.assertEqual(3, len(users_collection))
        for user in users_collection:
            name = user.name
            self.assertIsNotNone(name)
        count += dbms_fw.get_queries_amount("loading_names")
        self.assertEqual(count, connection.count_queries())

        for user in users_collection:
            email = user.account.email
            self.assertIsNotNone(email)
        count += dbms_fw.get_queries_amount("loading_accounts")
        self.assertEqual(count, connection.count_queries())

        for user in users_collection:
            tags_names = [tag.name for tag in user.tags]
            self.assertTrue(len(tags_names) > 0)
        count += dbms_fw.get_queries_amount("loading_tags")
        self.assertEqual(count, connection.count_queries())             # Потрачено три запроса


class RecordModelTest(unittest.TestCase):
    """ Модульные тесты для класса TableModel """
    def setUp(self):
        # noinspection PyPep8Naming
        self.maxDiff = None

    @for_all_dbms
    def test_load_by_constructor(self, dbms_fw):
        """ Для инициализации модели можно передать словарь с данными в конструктор """
        users = dbms_fw.get_new_users_collection_instance()
        self.assertEqual(0, users.count())

        user = dbms_fw.get_new_user_instance({"name": "Андрей", "age": 25})
        self.assertEqual("Андрей", user.name)
        self.assertEqual(25, user.age)
        user.save()
        self.assertEqual(1, users.count())

        users.delete()
        self.assertEqual(0, users.count())
        user = dbms_fw.get_new_user_instance({"uid": 1, "name": "Андрей", "age": 25}, True)
        self.assertEqual("Андрей", user.name)
        self.assertEqual(25, user.age)
        user.save()
        self.assertEqual(0, users.count())

    @for_all_dbms
    def test_get_data_and_stringify(self, dbms_fw):
        """
        Проверим метод формирования словаря для передачи мапперу
        Какие бы не были определены аттрибуты у объекта, в маппер должны идти только те, которые определены у маппера
        """
        user = dbms_fw.get_new_user_instance()
        user.name = "Андрей"
        user.age = 99
        user.register_date = date(2013, 9, 8)
        user.account = dbms_fw.get_new_account_instance()
        user.account.email = "andrey.yurjev@gmail.com"
        tag1 = dbms_fw.get_new_tag_instance()
        tag1.name = "FirstTag"
        tag2 = dbms_fw.get_new_tag_instance()
        tag2.name = "SecondTag"
        user.tags = [tag1, tag2]
        user.garbage_garbage_garbage = "garbage everywhere!"

        # Проверим базовый режим работы метода get_data()
        # Он должен возвращать вложенные объекты в виде объектов
        data = user.get_data()
        # Сперва проверим, как представлено свойства account - это должен быть экземпляр класса Account:
        self.assertTrue(isinstance(data["account"], dbms_fw.get_new_account_instance().__class__))
        self.assertTrue(isinstance(data["tags"][0], dbms_fw.get_new_tag_instance().__class__))
        self.assertTrue(isinstance(data["tags"][1], dbms_fw.get_new_tag_instance().__class__))
        self.assertEqual(data["tags"][0].name, "FirstTag")
        self.assertEqual(data["tags"][1].name, "SecondTag")

        # Заменим уже проверенное значение, так как нельзя предугадать адрес ячейки памяти объекта
        data["account"] = "checked"
        data["tags"] = "checked"

        # Теперь остальные поля
        self.assertEqual(
            {
                "uid": None,
                "name": "Андрей",
                "age": 99,
                "documents": [],
                "passport": None,
                "is_system": None,
                "latitude": None,
                "register_date": date(2013, 9, 8),
                "register_time": None,
                "register_datetime": None,
                'account': "checked",
                'tags': "checked",
                "profile": None,
                "statuses": []
            },
            data
        )

        self.assertDictEqual({"age": 99, "name": "Андрей"}, user.get_data(["age", "name"]))

        # Теперь проверим метод stringify:
        self.assertEqual(
            {
                "uid": None,
                "name": "Андрей",
                "age": 99,
                'new_property': 9900,               # Здесь обращаем внимание на новое поле, которого нет в маппере
                "is_system": None,
                "latitude": None,
                "register_date": date(2013, 9, 8),
                "register_time": None,
                "register_datetime": None,
                'account': {"email": "andrey.yurjev@gmail.com", 'id': None, 'phone': None},
                'tags': [
                    {"id": None, "name": "FirstTag", "weight": None},
                    {"id": None, "name": "SecondTag", "weight": None}
                ],
                "profile": None,
                "statuses": [],
                "documents": [],
                "passport": None
            },
            user.stringify())

        # Теперь проверим ограничение полей при стрингификации:
        self.assertEqual(
            {
                "uid": None,
                "name": "Андрей",
                "age": 99,
            },
            user.stringify(["uid", "name", "age"])
        )
        self.assertEqual(
            {
                "name": "Андрей",
                "account": {"email": "andrey.yurjev@gmail.com"},
                "new_property": 9900,
                'tags': [
                    {"name": "FirstTag"},
                    {"name": "SecondTag"}
                ]
            },
            user.stringify((["name", "account", "tags", "new_property"], {"account": ["email"], "tags": ["name"]}))
        )

    @for_all_dbms
    def test_save(self, dbms_fw):
        """
        Проверим работу метода save()
        Он должен работать в двух режимах:
        1) На новом объекте - тогда объект добавляется в коллекцию
        2) На существующем в коллекции объекте - тогда обновляем данные
        И учитывать следующую особенность:
        3) Если добавляется собранный в ручную объект, то прежде чем добавить, нужно убедиться в отсутствии дубликатов
        """
        users = dbms_fw.get_new_users_collection_instance()
        self.assertEqual(0, users.count())

        # 1)
        user = dbms_fw.get_new_user_instance()
        user.name = "Андрей"
        user.age = 99
        user.save()
        # После save() в таблице users появляется запись:
        self.assertEqual(1, users.count())
        self.assertIsNotNone(user.uid)
        self.assertEqual(["Андрей"], list(users.get_property_list("name", {"age": 99})))

        # 2)
        user.name = "Не Андрей"
        user.save()
        # После save() в таблице users остается одна единственная запись:
        self.assertEqual(1, users.count())
        self.assertIsNotNone(user.uid)
        self.assertEqual(["Не Андрей"], list(users.get_property_list("name", {"age": 99})))

        # 3)
        new_user = dbms_fw.get_new_user_instance()
        new_user.name = "Не Андрей"
        new_user.save()
        self.assertEqual(2, users.count())

    @for_all_dbms
    def test_remove(self, dbms_fw):
        """ Проверим работу метода удаления объекта """
        users = dbms_fw.get_new_users_collection_instance()
        self.assertEqual(0, users.count())

        user = dbms_fw.get_new_user_instance()
        user.name = "Андрей"
        user.age = 99
        user.save()
        # После save() в таблице users появляется запись:
        self.assertEqual(1, users.count())
        user.remove()
        self.assertEqual(0, users.count())

    @for_all_dbms
    def test_validate(self, dbms_fw):
        """
        Проверим работу валидации объектов перед сохранением в базу
        В настройках модели User должно быть предусмотрено, что свойство count не должно быть равно 42
        Попробуем нарушить это требование
        """
        users = dbms_fw.get_new_users_collection_instance()
        self.assertEqual(0, users.count())

        user = dbms_fw.get_new_user_instance()
        user.name = "Андрей"
        user.age = 42
        self.assertRaises(Exception, user.save)
        # При этом запись в БД не добавляется:
        self.assertEqual(0, users.count())

    @for_all_dbms
    def test_load_by_primary(self, dbms_fw):
        """
        Проверим корректность работы метода загрузки по первичному ключу
        Суть его работы в том, что после непосрдественного метода должна происходить отложенная инициализация объекта,
        а реальная загрузка должна происходить только тогда, когда у объекта запрашиваются данные
        """
        users = dbms_fw.get_new_users_collection_instance()
        self.assertEqual(0, users.count())

        user = dbms_fw.get_new_user_instance()
        user.name = "Андрей"
        user.age = 12
        user_id = user.save()
        self.assertEqual(1, users.count())

        user2 = dbms_fw.get_new_user_instance()
        initial_name = user2.__dict__.get("name")
        user2.load_by_primary(user_id)
        name_after_loading = user2.__dict__.get("name")
        self.assertEqual(initial_name, name_after_loading)

        self.assertEqual("Андрей", user2.name)
        name_after_access = user2.__dict__.get("name")
        count_after_access = user2.__dict__.get("age")
        self.assertEqual("Андрей", name_after_access)
        self.assertEqual(12, count_after_access)


if __name__ == "__main__":
    unittest.main()