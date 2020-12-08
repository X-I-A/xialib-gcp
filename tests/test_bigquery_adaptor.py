import os
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
    yield adaptor


def test_simple_flow(adaptor: BigQueryAdaptor):
    adaptor.drop_table(table_id)
    adaptor.drop_table(new_table_id)
    assert adaptor.create_table(table_id, {}, field_data)
    assert adaptor.upsert_data(table_id, field_data, data_02)
    # query_job = adaptor.connection.query(sql_count)
    # for result in query_job.result():
    assert adaptor.rename_table(table_id, new_table_id)
    info = adaptor.get_ctrl_info(new_table_id)
    assert info['TABLE_ID'] == new_table_id
    adaptor.set_ctrl_info(new_table_id, fieldlist=list(dict()))
    assert adaptor.load_raw_data(new_table_id, table_id, dict())
    assert not adaptor.insert_raw_data(table_id, list(dict()), "error data")
    assert not adaptor.insert_raw_data(table_id, list(dict()), [{"id": 1}, {"id": 2}])

def test_exceptions(adaptor: BigQueryAdaptor):
    with pytest.raises(TypeError):
        adap = BigQueryAdaptor(connection=object(), project_id='dummy')

