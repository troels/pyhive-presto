from pyhive import sqlalchemy_presto
from sqlalchemy.dialects import registry
from . import nzpresto


def register_nzpresto_dialect():
    registry.register('nzpresto', __name__, 'NZPrestoDialect')


class NZPrestoDialect(sqlalchemy_presto.PrestoDialect):
    name = 'nzpresto'

    @classmethod
    def dbapi(cls):
        return nzpresto

    def do_begin(self, connection):
        connection.begin()
