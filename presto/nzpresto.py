
import logging
import urllib.parse as urlparse

from uuid import uuid4
from pyhive import common, presto
from pyhive.presto import FIXED_INT_64, VARIABLE_BINARY, DOUBLE, BOOLEAN

__all__ = ['FIXED_INT_64', 'VARIABLE_BINARY', 'DOUBLE', 'BOOLEAN',
           'apilevel', 'threadsafety', 'paramstyle',
           'Connection']

# PEP 249 module globals
apilevel = '2.0'
threadsafety = 2  # Threads may share the module and connections.
# Python extended format codes, e.g. ...WHERE name=%(name)s
paramstyle = 'pyformat'

_escaper = common.ParamEscaper()
_logger = logging.getLogger(__name__)


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)


class CurrentTransaction:
    def __init__(self):
        self.transaction_id = None

    def is_active(self):
        return self.transaction_id is not None

    def set_transaction_id(self, transaction_id):
        self.transaction_id = transaction_id

    def reset_transaction_id(self):
        self.transaction_id = None

    def get_id(self):
        return 'none' if self.transaction_id is None else self.transaction_id


class Connection(object):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self.current_transaction = CurrentTransaction()
        self._kwargs.update({'current_transaction': self.current_transaction})

    def close(self):
        """Presto does not have anything to close"""
        # TODO cancel outstanding queries?
        if self.current_transaction.is_active():
            print("Committing")
            self.commit()

    def commit(self):
        if self.current_transaction.is_active():
            cursor = self.cursor()
            cursor.execute("commit")
            cursor.fetchall()

    def cursor(self):
        """Return a new :py:class:`Cursor` object using the connection."""
        return Cursor(*self._args, **self._kwargs)

    def rollback(self):
        if self.current_transaction.is_active():
            cursor = self.cursor()
            cursor.execute("rollback")
            cursor.fetchall()

    def begin(self):
        cursor = self.cursor()
        cursor.execute("start transaction")
        cursor.fetchall()


class Cursor(presto.Cursor):
    def __init__(self, *args, **kwargs):
        self.current_transaction = kwargs.pop('current_transaction')
        super().__init__(*args, **kwargs)

    def execute(self, operation, parameters=None):
        """Prepare and execute a database operation (query or command).

        Return values are not defined.
        """
        headers = {
            'X-Presto-Catalog': self._catalog,
            'X-Presto-Schema': self._schema,
            'X-Presto-Source': self._source,
            'X-Presto-User': self._username,
            'X-Presto-Transaction-Id': self.current_transaction.get_id()
        }

        if self._session_props:
            headers['X-Presto-Session'] = ','.join(
                '{}={}'.format(propname, propval)
                for propname, propval in self._session_props.items()
            )

        # Prepare statement
        if parameters is None:
            sql = operation
        else:
            sql = operation % _escaper.escape_args(parameters)

        self._reset_state()

        self._state = self._STATE_RUNNING
        url = urlparse.urlunparse((
            self._protocol,
            '{}:{}'.format(self._host, self._port), '/v1/statement', None, None, None))
        _logger.info('%s', sql)
        _logger.debug("Headers: %s", headers)
        print(headers)
        print(sql)
        response = self._requests_session.post(
            url, data=sql.encode('utf-8'), headers=headers, **self._requests_kwargs)
        self._process_response(response)

    def _process_response(self, response):
        """Given the JSON response from Presto's REST API, update the internal state with the next
        URI and any data from the response
        """
        if 'X-Presto-Started-Transaction-Id' in response.headers:
            self.current_transaction.set_transaction_id(
                response.headers['X-Presto-Started-Transaction-Id'])
        if 'X-Presto-Clear-Transaction-Id' in response.headers:
            self.current_transaction.reset_transaction_id()
        super()._process_response(response)
