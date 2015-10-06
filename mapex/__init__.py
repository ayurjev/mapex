__version__ = '0.1'

from .Sql import Adapter as DatabaseClient
from .Mappers import SqlMapper, NoSqlMapper
from .Models import TableModel as CollectionModel, RecordModel as EntityModel, Transaction
from .Models import EmbeddedObject, EmbeddedObjectFactory
from .Exceptions import DublicateRecordException
from .Pool import Pool

from .Adapters import MySqlDbAdapter as MySqlClient, PgSqlDbAdapter as PgSqlClient, \
    MsSqlDbAdapter as MsSqlClient, MongoDbAdapter as MongoClient
