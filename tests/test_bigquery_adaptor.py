import os
import time
import json
import pytest
import google.auth
from google.cloud import bigquery
from xialib_gcp import BigQueryAdaptor

with open(os.path.join('.', 'input', 'person_simple', 'schema.json'), encoding='utf-8') as fp:
    field_data = json.load(fp)

with open(os.path.join('.', 'input', 'person_simple', '000002.json'), encoding='utf-8') as fp:
    data_02 = json.load(fp)

table_id = "..test_01.simple_person"
new_table_id = "..test_01.simple_person_2"

sql_count = "SELECT COUNT(*) FROM `x-i-a-test.test_01.simple_person`"


@pytest.fixture(scope='module')
def adaptor():
    conn = bigquery.Client()
    project_id = google.auth.default()[1]
    adaptor = BigQueryAdaptor(connection=conn, project_id=project_id)
    adaptor.create_table(BigQueryAdaptor._ctrl_table_id, '', dict(), BigQueryAdaptor._ctrl_table)
    adaptor.drop_table(new_table_id)
    yield adaptor
    # adaptor.drop_table(BigQueryAdaptor._ctrl_table_id)


def test_simple_flow(adaptor: BigQueryAdaptor):
    assert adaptor.create_table(table_id, '20200101000000000000', {}, field_data)
    assert adaptor.alter_column(table_id, {'type_chain': ['char', 'c_8']}, {'type_chain': ['char', 'c_9']})
    assert adaptor.upsert_data(table_id, field_data, data_02)
    assert not adaptor.insert_raw_data(table_id, list(dict()), [{}])
    assert not adaptor.insert_raw_data(table_id, list(dict()), [{"id": 1}, {"id": 2}])
    assert adaptor.rename_table(table_id, new_table_id)
    adaptor.set_ctrl_info(table_id, fieldlist=list(dict()))
    assert adaptor.load_log_data(new_table_id)
    adaptor.drop_table(table_id)

def test_escape_column_name(adaptor: BigQueryAdaptor):
    assert adaptor._escape_column_name(r"/TEST/Hello") == "_TEST_Hello"
    assert adaptor._escape_column_name(r"0Hello") == "_0Hello"
    assert adaptor._escape_column_name(r"_TABLE_Hello") == "__TABLE_Hello"
    long_str = "".join([str(x) for x in range(100)])
    assert len(adaptor._escape_column_name(long_str)) == 128

def test_dummy_log_func(adaptor: BigQueryAdaptor):
    assert adaptor.get_log_table_id('dummy') == ''
    assert adaptor.get_log_info('dummy') == []

def test_exceptions(adaptor: BigQueryAdaptor):
    with pytest.raises(TypeError):
        adap = BigQueryAdaptor(connection=object(), project_id='dummy')

