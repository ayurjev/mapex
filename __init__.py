from mapex.core.Sql import Adapter as DatabaseClient
from mapex.core.Mappers import SqlMapper, NoSqlMapper
from mapex.core.Models import TableModel as CollectionModel, RecordModel as EntityModel
from mapex.core.Models import EmbeddedObject, EmbeddedObjectFactory
from mapex.dbms.Adapters import PgSqlDbAdapter as PgSqlClient, MySqlDbAdapter as MySqlClinet, \
    MsSqlDbAdapter as MsSqlClient, MongoDbAdapter as MongoClient

from mapex.core.Exceptions import DublicateRecordException
