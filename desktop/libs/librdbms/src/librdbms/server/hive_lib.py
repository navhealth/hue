from decimal import Decimal
from datetime import datetime
import logging
from time import sleep
from pyhive import presto
from pyhive.presto import Cursor
import traceback
from requests.auth import HTTPBasicAuth

try:
    from pyhive import hive as Database
except ImportError, e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("Error loading pyhive hive module: %s" % e)

from librdbms.server.rdbms_base_lib import BaseRDBMSDataTable, BaseRDBMSResult, BaseRDMSClient

LOG = logging.getLogger(__name__)

# MAP OF RETRY RULES
# KEY is the exception string to search for
_RETRY_MAP = {
    'Error getting permissions for s3': {'tot': 6, 'delay': 10},
}


class DataTable(BaseRDBMSDataTable): pass


class Result(BaseRDBMSResult): pass


class MaxRetriesReached(Exception): pass


class HiveClient(BaseRDMSClient):

  data_table_cls = DataTable
  result_cls = Result

  def __init__(self, *args, **kwargs):
    super(HiveClient, self).__init__(*args, **kwargs)
    params = self._conn_params
    conn = Database.connect(
      host=params["host"],
      port=params["port"],
      username=params["username"],
      password=params["password"],
      database=params["database"],
      auth='LDAP',
    )

    self.connection = ConnectionWrapper(conn)


  @property
  def _conn_params(self):
    params = {
      'host': str(self.query_server['server_host']),
      'port': self.query_server['server_port'] == 0 and 10000 or self.query_server['server_port'],
      'username': str(self.query_server['username']),
      'password': str(self.query_server['password']),
      'database': str(self.query_server['name']),
    }

    if self.query_server['options']:
      params.update(self.query_server['options'])

    return params

  def use(self, database):
    # do nothing, leave default schema alone
    pass


  def execute_statement(self, statement, fetch_max=None):
    cursor = self.connection.cursor()
    statement = "--USER: {user} \n{sql}".format(user=str(self.user) + '@HUE',
                                                sql=statement)
    cursor.execute(statement)

    if cursor.description:
      columns = [{'name': column[0], 'type': column[1]}
                 for column in cursor.description]
    else:
      columns = []
    return self.data_table_cls(cursor, columns, fetch_max=fetch_max)


  def get_databases(self):
    # List all the schemas in the database
    try:
      cursor = self.connection.cursor()
      cursor.execute('SHOW databases')

      return [row[0] for row in cursor.fetchall()]
    except Exception:
      LOG.exception('Failed to SHOW databases')
      return [self._conn_params['database']]


  def get_tables(self, database, table_names=[]):
    cursor = self.connection.cursor()
    query = """
      SHOW tables
      IN %s
    """ % database

    cursor.execute(query)

    return [row[0] for row in cursor.fetchall()]


  def get_columns(self, database, table, names_only=True):
    cursor = self.connection.cursor()
    query = """
      DESCRIBE
        %(database)s.%(table)s
    """ % {'table': table, 'database': database}

    cursor.execute(query)


    columns = []
    for row in cursor.fetchall():
      if row[0] is None or row[0] == '':
        break
      if names_only:
        columns.append(row[0])
      else:
        columns.append(dict(name=row[0], type=row[1], comment=''))
    return columns

  def get_sample_data(self, database, table, column=None, limit=100):
    column = '"%s"' % column if column else '*'
    statement = 'SELECT %s FROM "%s"."%s" LIMIT %d' % (column, database, table, limit)
    return self.execute_statement(statement)


class ConnectionWrapper(object):

  def __init__(self, connection):
    self._connection = connection

  # passthrough call
  def close(self):
    self._connection.close()

  # passthrough call
  def commit(self):
    self._connection.commit()

  # intercept to wrap the cursor with our wrapper
  def cursor(self, record_as_dict=False):
    cw = CursorWrapper(self._connection, record_as_dict)
    return cw

  # hive doesn't have transactions in the way we'd want so override here
  def rollback(self):
    pass


class CursorWrapper(object):

  def __init__(self, connection, record_as_dict=False):

    # initialize custom class vars
    self._connection = connection
    self._column_names = []
    self._column_types = []
    self._description = None
    self._column_types = None
    self._column_types = None
    self._results = None
    self._rownumber = 0


  @property
  def description(self):
      return self._description

  def close(self):
      """
      simulated cursor method.  do nothing.
      :return:
      """
      pass

  def execute(self, operation, parameters=None, **kwargs):
      """
      Hive requires closure of each operation.  Normnally, this would means
      that everywhere this driver is used, the caller must close both the
      cursor returned by the execute method as well as the connection.  To
      simplify things, this cursor wrapper will close the cursor
      immediately and return a pseudo-cursor instead.

      This is OK because we only use Hive for DDL operations and therefore
      do not need to support server side cursor iteration.
      :param operation: sql statement
      :param parameters: sql params (optional)
      :return:
      """
      # get cursor
      with self._connection.cursor() as cur:
          error_map = {}
          error_list = []

          while True:

              try:
                  # call super to execute statement
                  cur.execute(operation=operation,
                              parameters=parameters)
                  break
              except Exception as ex:
                  do_retry = False
                  for exc_msg, retry_opts in _RETRY_MAP.iteritems():
                      if exc_msg in ex.args[0].status.errorMessage:
                          tot = retry_opts['tot']
                          delay = retry_opts['delay']

                          error_map[exc_msg] = error_map.get(exc_msg, 0) + 1
                          cnt = error_map[exc_msg]

                          # attempts remaining
                          if cnt <= tot:
                              error_message = ('Execution attempt {cnt} of {tot} '
                                               'encountered error: {exc}. '
                                               'Sleep for {delay} seconds '
                                               'and retry.'.format(cnt=cnt,
                                                                   tot=tot,
                                                                   exc=exc_msg,
                                                                   delay=delay))
                              error_list.append({'retry_number': cnt, 'message': error_message})
                              LOG.warning(error_message)

                              # sleep then execute
                              sleep(delay)
                              do_retry = True
                              break
                  # if exception does not match registered exceptions, re-raise as-is
                  if not do_retry:
                      raise

          # set metadata
          self._set_cursor_metadata(cur.description)

          # return all results into self._results
          try:
              self._results = cur.fetchall()
          except:
              pass

  @property
  def rownumber(self):
      """This read-only attribute should provide the current 0-based index of the cursor in the
      result set.
      The index can be seen as index of the cursor in a sequence (the result set). The next fetch
      operation will fetch the row indexed by ``rownumber`` in that sequence.
      """
      return self._rownumber

  def __iter__(self):
      return self

  def next(self):
      """Return the next row from the currently executing SQL statement using the same semantics
      as :py:meth:`fetchone`. A ``StopIteration`` exception is raised when the result set is
      exhausted.
      """
      one = self.fetchone()
      if one is None:
          raise StopIteration
      else:
          return one

  def fetchmany(self, size=None):
    rows = []
    rownum = 0
    while rownum < size:
      rownum += 1
      row = self.fetchone()
      if row:
        rows.append(row)
      else:
        break
    return rows

  def fetchone(self):
      """
      retrieve row from self._results.  if record_as_dict requested, then
      pass row to dict formatter else just return the row

      :return:
      """
      if self._rownumber < len(self._results):
          row = self._results[self._rownumber]
          self._rownumber += 1
      else:
          row = None

      return row

  def fetchall(self):
      return [rec for rec in self]

  def _set_cursor_metadata(self, cursor_description):
      if cursor_description:
          self._description = cursor_description
          self._column_names = []
          for desc in cursor_description:
              col_name = desc[0].split('.')
              self._column_names.append(col_name[0])
          self._column_names = tuple(self._column_names)
          self._column_types = tuple([desc[1] for desc in cursor_description])
      else:
          self._description = cursor_description
          self._column_names = None
          self._column_types = None

