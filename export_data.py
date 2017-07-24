# File name: export_data.py
# Author: Le Kien Truc <afterlastangel@gmail.com>
# Copyright: Richard Rowley - folddigital.co.uk
# Python Version: 2.7

import binascii
import csv
import pymysql.cursors
import os

import arrow

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from gcloud.bigquery.job import CreateDisposition, WriteDisposition


import datetime

# Mysql configuration
DB_HOST = ''
DB_USER = ''
DB_PASSWORD = ''
DB_NAME = ''
FILE_PREFIX = '/tmp/'
TABLE_PREFIX = 'piwik_'

# Piwik configuration
SITE_ID = 1

# BigQuery configuration
PROJECT_ID = ''
DATASET_NAME = ''
SECRET_SERVICE_ACCOUNT_KEY = ''  # noqa


START_DATE = None
LAST_ID_ACTION = None

# Comment out for first time running without table on dataset
# START_DATE = (2016, 8, 29)
# LAST_ID_ACTION = 0

os.environ['GCLOUD_PROJECT'] = PROJECT_ID
DEFAULT_DATA_TYPE = 'STRING'
BINARY_TYPES = [254]
MAPPING_DATA_TYPES = {
    0: 'FLOAT',
    1: 'INTEGER',
    2: 'INTEGER',
    3: 'INTEGER',
    4: 'FLOAT',
    5: 'FLOAT',
    7: 'TIMESTAMP',
    8: 'INTEGER',
    9: 'INTEGER',
    10: 'DATE',
    11: 'TIME',
    12: 'DATETIME',
    15: 'STRING',
    249: 'STRING',
    250: 'STRING',
    251: 'STRING',
    253: 'STRING',
    254: 'STRING'
}


def convert_date_time_between_timezones(date_time, from_tz, to_tz):
    return arrow.get(date_time, from_tz).to(
        to_tz).datetime.replace(tzinfo=None)


def convert_from_utc(date_time, tz):
    if date_time is None:  # pragma: no cover
        return None
    return convert_date_time_between_timezones(date_time, 'UTC', tz)


def convert_to_utc(date_time, tz):
    return convert_date_time_between_timezones(date_time, tz, 'UTC')


class BasePiwikDataTableProcessor(object):

    def __init__(self, connection, bigquery_client):
        self.connection = connection
        self.bigquery_client = bigquery_client

    @property
    def end_time(self):
        return datetime.datetime.now() - datetime.timedelta(days=1)

    @property
    def idaction(self):
        if LAST_ID_ACTION is not None:
            return LAST_ID_ACTION
        query = self.bigquery_client.run_sync_query(self.last_idaction_sql)
        query.use_legacy_sql = False
        query.run()
        rows = query.rows
        return rows[0][0]

    @property
    def start_time(self):
        if START_DATE is not None:
            return datetime.datetime(*START_DATE)
        query = self.bigquery_client.run_sync_query(self.last_time_sql)
        query.use_legacy_sql = False
        query.run()
        data = query.rows[0][0]
        if data is None:
            data = datetime.datetime.now() - datetime.timedelta(days=10)
        return data

    def full_path_name(self):
        return FILE_PREFIX + self.filename

    def get_type(self, sql_type):
        if sql_type in MAPPING_DATA_TYPES:
            return MAPPING_DATA_TYPES[sql_type]
        else:
            return DEFAULT_DATA_TYPE

    def process_binary(self, data, sql_type, description):
        result = []
        for col in range(len(data)):
            if (description[col][1] in BINARY_TYPES or description[col][0] == "location_ip") and data[col] is not None:  # noqa
                result.append(binascii.b2a_hex(data[col]))
            else:
                result.append(data[col])
        return result

    """
    Return
    filename
    last_id
    schema
    table_name
    """
    def write_sql_to_file(
            self, sql, bind, last_id_name=None,
            with_header=True, delimiter=',',
            quotechar='"', quoting=csv.QUOTE_NONNUMERIC):
        conn = self.connection
        cur = conn.cursor(pymysql.cursors.SSCursor)
        cur.execute(sql, bind)
        fields_name = []
        schemas = []
        count = 0
        last_id_column = None
        last_id = None
        sql_type = []
        description = cur.description
        for column in description:
            fields_name.append(column[0])
            sql_type.append(column[1])
            schemas.append(
                SchemaField(column[0], self.get_type(column[1])))
            if column[0] == last_id_name:
                last_id_column = count
            count += 1

        ofile = open(self.full_path_name(), 'wb')
        csv_writer = csv.writer(
            ofile, delimiter=delimiter, quotechar=quotechar, quoting=quoting)
        if with_header:
            csv_writer.writerow(fields_name)
        while True:
            results = cur.fetchmany(size=1000)
            if len(results) != 0:
                for result in results:
                    data = self.process_binary(result, sql_type, description)
                    csv_writer.writerow(data)
                    if last_id_column is not None:
                        last_id = result[last_id_column]
            else:
                break
        cur.close()
        ofile.close()
        return schemas, last_id

    def execute(self):
        last_id_name = None
        if hasattr(self, 'last_id_name'):
            last_id_name = self.last_id_name
        schemas, last_id = self.write_sql_to_file(
            self.sql,
            self.bind,
            last_id_name, with_header=False
        )
        return self.full_path_name(), schemas, last_id, self.table_name


class VisitProcessor(BasePiwikDataTableProcessor):

    @property
    def sql(self):
        return "SELECT * FROM `%slog_visit`" % TABLE_PREFIX + " WHERE `visit_first_action_time` > %s AND `visit_first_action_time` <= %s"  # noqa

    @property
    def last_time_sql(self):
        return "SELECT max(visit_first_action_time) from `%s.%s`" % (
            DATASET_NAME,
            self.table_name)

    @property
    def bind(self):
        return (self.start_time, self.end_time)

    @property
    def table_name(self):
        return 'log_visit'

    @property
    def filename(self):
        return 'log_visit.csv'


class ActionProcessor(BasePiwikDataTableProcessor):

    @property
    def sql(self):
        return "SELECT * FROM `%slog_action`" % TABLE_PREFIX + " WHERE `idaction` > %s"  # noqa

    @property
    def last_idaction_sql(self):
        return "SELECT max(idaction) from `%s.%s`" % (
            DATASET_NAME,
            self.table_name)

    @property
    def bind(self):
        return (self.idaction)

    @property
    def table_name(self):
        return 'log_action'

    @property
    def filename(self):
        return 'log_action.csv'

    @property
    def last_id_name(self):
        return 'idaction'


class LinkVisitActionProcessor(BasePiwikDataTableProcessor):

    @property
    def sql(self):
        return "SELECT * FROM `%slog_link_visit_action`" % TABLE_PREFIX + " WHERE `server_time` > %s AND `server_time` < %s"  # noqa

    @property
    def last_time_sql(self):
        return "SELECT max(server_time) from `%s.%s`" % (
            DATASET_NAME,
            self.table_name)

    @property
    def bind(self):
        return (self.start_time, self.end_time)

    @property
    def table_name(self):
        return 'log_link_visit_action'

    @property
    def filename(self):
        return 'log_link_visit_action.csv'


class ConversionProcessor(BasePiwikDataTableProcessor):

    @property
    def sql(self):
        return "SELECT * FROM `%slog_conversion`" % TABLE_PREFIX + " WHERE `server_time` > %s AND `server_time` <= %s"  # noqa

    @property
    def last_time_sql(self):
        return "SELECT max(server_time) from `%s.%s`" % (
            DATASET_NAME,
            self.table_name)

    @property
    def bind(self):
        return (self.start_time, self.end_time)

    @property
    def table_name(self):
        return 'log_conversion'

    @property
    def filename(self):
        return 'log_conversion.csv'


class ConversionItemProcessor(BasePiwikDataTableProcessor):

    @property
    def sql(self):
        return "SELECT * FROM `%slog_conversion_item`" % TABLE_PREFIX + " WHERE `server_time` > %s AND `server_time` <= %s"  # noqa

    @property
    def last_time_sql(self):
        return "SELECT max(server_time) from `%s.%s`" % (
            DATASET_NAME,
            self.table_name)

    @property
    def bind(self):
        return (self.start_time, self.end_time)

    @property
    def table_name(self):
        return 'conversion_item'

    @property
    def filename(self):
        return 'conversion_item.csv'


class BigQueryPluginImporter(object):

    def __init__(self):
        self.connection = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME,
            cursorclass=pymysql.cursors.SSCursor)
        self.bigquery_client = bigquery.Client.from_service_account_json(
            SECRET_SERVICE_ACCOUNT_KEY)

    def import_to_big_query(
            self, table_name, filename, schema):
        dataset = self.bigquery_client.dataset(DATASET_NAME)
        table = dataset.table(table_name, schema)
        job = table.upload_from_file(
            open(filename, 'rb'),
            encoding='UTF-8',
            source_format='CSV',
            write_disposition=WriteDisposition.WRITE_APPEND,
            create_disposition=CreateDisposition.CREATE_IF_NEEDED,
            allow_jagged_rows=True,
            allow_quoted_newlines=True
        )
        print job.name
        print job._properties['configuration']['load']['destinationTable']

    def execute(self):
        ProcessorKlasses = [
            VisitProcessor,
            ActionProcessor,
            LinkVisitActionProcessor,
            ConversionProcessor,
            ConversionItemProcessor]
        for ProcessorKlass in ProcessorKlasses:
            processor = ProcessorKlass(self.connection, self.bigquery_client)
            full_path, schemas, last_id, table_name = processor.execute()
            self.import_to_big_query(table_name, full_path, schemas)


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    _log = logging.getLogger(__name__)
    # TODO Run the script by parameter instead using CONST
    BigQueryPluginImporter().execute()
