""" Модели """

import os
from mapex import EntityModel, CollectionModel, SqlMapper, Pool, MySqlClient

# Configure db-connection pool:
pool = Pool(
    MySqlClient, (
        os.environ.get('MYSQL_HOST', 'db'),
        os.environ.get('MYSQL_PORT', 3306),
        os.environ.get('MYSQL_USER', "root"),
        os.environ.get('MYSQL_PASS', ""),
        os.environ.get('MYSQL_DBNAME', "dbname")
    )
)


# Implement mapper:
class UsersMapper(SqlMapper):
    # bind this mapper with specific pool:
    pool = pool

    # implement single method in order to configure mapper:
    def bind(self):
        # set_new_item() should receive a type that would represent a single item.
        # Not an instance, type!
        self.set_new_item(User)

        # set_new_collection() should receive a type that would represent a collection of items (repository).
        # Not an instance, type!
        self.set_new_collection(Users)

        # define a name of the database table:
        self.set_collection_name("users")

        # define relations between model's props and database table's fields:
        self.set_map([
            self.int("id", "ID"),  # should be tinyint, smallint, int, etc in database
            self.str("name", "Name"),  # should be varchar, text, etc in database
            self.str("email", "Email"),
            self.str("phone", "Phone"),
            self.bool("active", "Active"),  # should be tinyint in database (0, 1)
            self.date("registred", "RegistredDate"),  # should be datetime in database (not timestamp)
            self.datetime("updated", "Updated"),  # should be date in database (not timestamp)
        ])

        # Make sense only if there is no primary key defined in database.
        # In this case you have to call set_primary() method:
        # self.set_primary("id")


# define a class for single item:
class User(EntityModel):
    # define a mapper:
    mapper = UsersMapper

    # you can daefine your own properties on this model, but you can not use them in db queries.
    @property
    def account(self):
        return {}

    # you can define some method that would return dict-representation of the model with given fields (including props)
    def describe(self):
        return self.stringify(["id", "name", "email", "phone", "active", "phone", "updated", "account"])


# define a class for collection (repository) of items:
class Users(CollectionModel):
    # define a mapper:
    mapper = UsersMapper
    # Usually it goes without additional logic... It is up to you, though...


# USING MODElS DEFINED:
if __name__ == "__main__":

    # BASIC METHODS ON REPOSITORY OBJECT:

    # create repository instance:
    users = Users()

    # get an item from repository:
    user_Ivan = Users().get_item({"id": 42})  # this should return either a single User() object or None if not found

    # FOLLOWING IS WRONG!
    # You gonna get an exception, because obviously there would be more than one record affected by this query:
    old_users = Users().get_item({"age": ("gte", 18)})  # Exception!

    # a correct way to get a list of users:
    old_users = Users().get_items({"age": ("gte", 18)})  # This will return a list (may be empty) of users

    # get a quantity of records:
    users_total = Users().count()
    old_users_count = Users().count({"age": ("gte", 18)})

    # delete all unactive users:
    Users().delete({"active": False})

    # make all unactive users active again:
    Users().update({"active": True}, {"active": False})

    # DB QUERIES (EXTENDED):
    users.get_items({"name": "Ivan"})  # Equal
    users.get_items({"name": ("e", "Ivan")})  # Equal
    users.get_items({"name": ("ne", "Ivan")})  # Not Equal
    users.get_items({"name": ("ne", "Ivan")})  # Not Equal
    users.get_items({"age": ("gt", "18")})  # Greater than
    users.get_items({"age": ("gte", "18")})  # Greater than or equal
    users.get_items({"age": ("lt", "18")})  # Less than or equal
    users.get_items({"age": ("lte", "18")})  # Less than or equal
    users.get_items({"age": ("in", [5, 18, 42])})  # Value in a list
    users.get_items({"phone": ("exists", True)})  # If field is not null
    users.get_items({"and": [{"age": ("gt", 18)}, {"age": ("lt", 42)}]})  # between
    users.get_items({"or": [{"age": 18}, {"age": 42}]})  # 'OR' for the same field

    # DB QUERIES (MODIFIERS/OPTIONS):
    users.get_items({"age": ("gte", "18")}, {"limit": 100})  # Limit
    users.get_items({"age": ("gte", "18")}, {"order": ("id", "ASC")})  # Ascending order
    users.get_items({"age": ("gte", "18")}, {"order": ("id", "DESC")})  # Descending order
    users.get_items({"age": ("gte", "18")}, {"skip": 50, "limit": 100})  # Offset/skip first 50, get next 100

    # HIGH PERFORMANCE QUERIES:
    # This would return a generator!! of dicts instead of models!!!
    users.generate_items({"age": ("gte", "18")}, {"limit": 100})

    # BASIC METHODS ON ITEM OBJECT:
    # get an instance of a User from repository (if exists) or create (prepare) new one:
    user = Users().get_item({"id": 42}) or User()
    # define/update new props:
    user.name = "Ivan"
    user.age = 42
    user.active = True
    # save changes:
    user.save()

    # delete user:
    user.remove()

    # get dict-representation:
    user.stringify(["id", "name"])  # any fields or properties. Will return something like {"id": 42, "name": "Ivan"}

    # Do something with transaction:
    with Users().pool.transaction as t:
        Users().delete()
        User({"id": 42, "name": "Ivan"}).save()
        if False:
            t.rollback()
        # it calls commit() automatically when leaving transaction scope without exceptions

    # Use raw sql-queries:
    # Try not to use this approach if not necessary...
    with Users().pool as db:
        db.execute_raw('''
            TRUNCATE `credentials`;
            TRUNCATE `notifications`;
            TRUNCATE `customers`;
            TRUNCATE `customer_phones`;
            TRUNCATE `customer_addresses`;
            TRUNCATE `customer_carts`;
            TRUNCATE `customer_carts_items`;
            TRUNCATE `catalog_items`;
            TRUNCATE `catalog_categories`;
            TRUNCATE `orders`;
        ''')
