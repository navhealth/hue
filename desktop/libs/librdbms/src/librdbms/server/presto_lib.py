from decimal import Decimal
from datetime import datetime
import logging
from time import sleep
from pyhive import presto
from pyhive.presto import Cursor
import traceback
from requests.auth import HTTPBasicAuth




try:
    from pyhive import presto as Database
except ImportError, e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("Error loading pyhive presto module: %s" % e)

from librdbms.server.rdbms_base_lib import BaseRDBMSDataTable, BaseRDBMSResult, BaseRDMSClient


LOG = logging.getLogger(__name__)

# MAP OF RETRY RULES
# KEY is the exception string to search for
_RETRY_MAP = {
  'Error committing write to Hive': {'tot': 60, 'delay': 5},
  # Guard against FileNotFoundException
  'Error opening Hive split': {'tot': 60, 'delay': 5},
  # Guard against FileNotFoundException
  'FileNotFoundException': {'tot': 60, 'delay': 5},  # Java class match
  'AmazonS3Exception': {'tot': 60, 'delay': 5}
}


class DataTable(BaseRDBMSDataTable): pass


class Result(BaseRDBMSResult): pass


class MaxRetriesReached(Exception): pass


class PrestoClient(BaseRDMSClient):
  """Same API as Beeswax"""

  data_table_cls = DataTable
  result_cls = Result

  def __init__(self, *args, **kwargs):
    super(PrestoClient, self).__init__(*args, **kwargs)
    params = self._conn_params
    conn = Database.connect(
      host=params['host'],
      port=params['port'],
      username=params['username'],
      schema=params["schema"],
      protocol=params['protocol'],
      requests_kwargs={
        'auth': HTTPBasicAuth(
          params['username'],
          params['password']
          ),
        'verify': params['verify']
      }
    )
    self.connection = ConnectionWrapper(conn)


  @property
  def _conn_params(self):
    params = {
      'host': self.query_server['server_host'],
      'port': self.query_server['server_port'] == 0 and 8887 or self.query_server['server_port'],
      'username': self.query_server['username'],
      'password': self.query_server['password'],
      'schema': self.query_server['name'],
      'protocol': 'https'
    }

    if self.query_server['options']:
      params.update(self.query_server['options'])

      # if verify (ssl cert) is not provided, default to false
      # if provided, check if vaue is "false" and set to bool
      # otherwise, use provided value
      if 'verify' not in params:
        params['verify'] = False
      elif str(params['verify']).lower() == 'false':
        params['verify'] = False

    return params


  def use(self, database):
    # do nothing, leave default schema alone
    pass


  def execute_statement(self, statement):
    cursor = self.connection.cursor()
    cursor.execute(statement)

    if cursor.description:
      columns = [column[0] for column in cursor.description]
    else:
      columns = []
    return self.data_table_cls(cursor, columns)


  def get_databases(self):
    # List all the schemas in the database
    try:
      cursor = self.connection.cursor()
      cursor.execute('SHOW schemas')

      return [row[0] for row in cursor.fetchall()]
    except Exception:
      LOG.exception('Failed to SHOW schemas')
      return [self._conn_params['schema']]


  def get_tables(self, database, table_names=[]):
    cursor = self.connection.cursor()
    query = """
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = '%s'
    """ % database
    if table_names:
      clause = ' OR '.join(["table_name LIKE '%%%(table)s%%'" % {'table': table} for table in table_names])
      query += ' AND (%s)' % clause
    query += """
      ORDER BY table_name
    """
    cursor.execute(query)

    return [row[0] for row in cursor.fetchall()]


  def get_columns(self, database, table, names_only=True):
    cursor = self.connection.cursor()
    query = """
      SELECT
          column_name as name,
          data_type as datatype
      FROM
          information_schema.columns
      WHERE
          table_schema = '%(database)s' and
          table_name = '%(table)s'
      ORDER BY ordinal_position
    """ % {'table': table, 'database': database}

    cursor.execute(query)

    if names_only:
      columns = [row[0] for row in cursor.fetchall()]
    else:
      columns = [dict(name=row[0], type=row[1], comment='') for row in cursor.fetchall()]
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
    pass

  # intercept to wrap the cursor with our wrapper
  def cursor(self):
    cw = CursorWrapper(*self._connection._args, **self._connection._kwargs)
    return cw

  # presto doesn't have transactions and will raise an exception if this is
  # called so we will override here
  def rollback(self):
    pass


class CursorWrapper(Cursor):

  def __init__(self, *args, **kwargs):

    # initialize custom class vars
    self._column_names = []
    self._column_types = []

    # initialize super
    super(CursorWrapper, self).__init__(*args, **kwargs)

  def execute(self, operation, parameters=None):
    """
    THIS IS THE METHOD WE HAVE TO OVERRIDE BECAUSE PRESTO IS BY DEFAULT
    AN ASYNCHRONOUS PROCESSOR OUR FRAMEWORK BY DEFAULT EXPECTS SYNCHRONOUS
    QUERY PROCESSING
    :param operation: sql statement
    :param parameters: sql params (optional)
    :return:
    """

    error_map = {}

    while True:
      try:
        super(CursorWrapper, self).execute(operation=operation,
                                           parameters=parameters)
        # call the description function to wait for the operation to
        # progress sufficiently far for us to be comfortable we can
        # return the function call
        # NOTE:  description works is that description waits until:
        #   - for select statements --> when first rows are available
        #       (description then returns row layout (columns) metadata)
        #   - for DML (e.g. insert) statements --> when operation complete
        #       (description function then returns # rows affected metadata
        desc = self.description
        self._set_cursor_metadata(desc)

        # check to see if this is a DML operation (desc just has one row
        # and one column (rows)). if so, then wait till query state is
        # "FINISHED" to be sure that data has been written to S3
        if desc:
          if len(desc) == 1 and desc[0][0] == 'rows':
            self._fetch_while(
              lambda: self._state != self._STATE_FINISHED
            )
        break
      except Exception as err:
        if not self._can_retry_query(err, error_map, operation):
          raise
        # else loop with while again

  def fetchone(self):
    """
    call the super's fetchone.  if record_as_dict requested, then
    pass row to dict formatter else just return the row
    NOTE:  next() in super calls fetchone() so only need this override.
           the reason for the reference to next() is that next() is
           the method called when the consumer of the cursor iterates
           to get rows:
              e.g. --> for row in cursor: ...
              e.g. [... for row in cursor]
    :return:
    """
    row = super(CursorWrapper, self).fetchone()
    if row:
      row = self._get_typed_row(row)

    return row

  def _set_cursor_metadata(self, cursor_description):
    if cursor_description:
      self._column_names = tuple([desc[0] for desc in cursor_description])
      self._column_types = tuple([desc[1] for desc in cursor_description])
    else:
      self._column_names = None
      self._column_types = None

  def _get_typed_row(self, row):
    """
    PyHive simply converts the json data from Presto's http response into a
    dict so additional casting is required for non-json types including:
        * Decimal
        * Date
        * Datetime
    :param row:
    :return:
    """
    typed_row = []
    typed_row_append = typed_row.append
    for idx, col_type in enumerate(self._column_types):
      val = row[idx]
      if val is not None:
        if 'decimal' in col_type:
          typed_val = Decimal(val)
        elif col_type == 'date':
          typed_val = datetime.strptime(val, '%Y-%m-%d').date()
        elif col_type == 'timestamp':
          typed_val = datetime.strptime(val, '%Y-%m-%d %H:%M:%S.%f')
        else:
          typed_val = val
      else:
        typed_val = val
      typed_row_append(typed_val)

    return tuple(typed_row)

  def _can_retry_query(self, exception, error_map, sql):
    op_tags = sql.split("\n")[0]
    if type(exception) == presto.DatabaseError:
      LOG.warning("DATABASE ERROR THROWN")
    else:
      LOG.warning("Unknown exception class {}".format(type(exception)))
      return False

    for exc_msg, retry_opts in _RETRY_MAP.iteritems():
      if exc_msg.lower() in str(exception.message).lower():
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
          LOG.warning(error_message)

          # sleep then retry
          sleep(delay)
          return True
        # attempts exceeded
        else:
          error_message = ('Maximum execution attempts exceeded '
                           'for exception: {exc}. '
                           'Exception detail: \n'
                           '{detail} \n'.format(exc=exc_msg,
                                                detail=exception.message))
          LOG.warning(error_message)
          return False
    LOG.warning("Exception not retried. {}".format(exception.message))

    return False
