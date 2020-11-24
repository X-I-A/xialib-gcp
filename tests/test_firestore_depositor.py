import os
import json
import datetime
import pytest
from google.cloud import firestore
from xialib import BasicTranslator
from xialib_gcp import FirestoreDepositor


@pytest.fixture(scope='module')
def depositor():
    depositor = FirestoreDepositor(db=firestore.Client())
    yield depositor

def add_aged_header(depositor):
    with open(os.path.join('.', 'input', 'person_complex', 'schema.json'), 'r') as f:
        body = json.load(f).pop('columns')
        start_seq = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
        header = {'topic_id': 'test-001', 'table_id': 'person_complex', 'start_seq': start_seq,
                  'age': '1', 'aged': 'true', 'merge_level': 9, 'merge_status': 'header', 'merge_key': start_seq,
                  'data_encode': 'flat', 'data_format': 'record', 'data_store': 'body'}
        depositor.add_document(header, body)

def add_normal_header(depositor):
    with open(os.path.join('.', 'input', 'person_complex', 'schema.json'), 'r') as f:
        body = json.load(f).pop('columns')
        start_seq = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
        header = {'topic_id': 'test-001', 'table_id': 'person_complex', 'start_seq': start_seq,
                  'age': '1', 'merge_level': 9, 'merge_status': 'header', 'merge_key': start_seq,
                  'data_encode': 'flat', 'data_format': 'record', 'data_store': 'body'}
        depositor.add_document(header, body)

def test_add_aged_header_document(depositor: FirestoreDepositor):
    add_aged_header(depositor)
    depositor.set_current_topic_table('test-001', 'person_complex')
    header_ref = depositor.get_table_header()
    assert header_ref is not None
    header_dict = depositor.get_header_from_ref(header_ref)
    header_data = depositor.get_data_from_header(header_dict)
    assert header_dict['topic_id'] == 'test-001'
    assert header_dict['aged'] == True
    assert len(header_data) == 14
    depositor.delete_documents([header_ref])
    header_ref = depositor.get_table_header()
    assert header_ref is None


def test_add_normal_header_document(depositor: FirestoreDepositor):
    add_normal_header(depositor)
    depositor.set_current_topic_table('test-001', 'person_complex')
    header_ref = depositor.get_table_header()
    assert header_ref is not None
    header_dict = depositor.get_header_from_ref(header_ref)
    header_data = depositor.get_data_from_header(header_dict)
    assert header_dict['topic_id'] == 'test-001'
    assert header_dict['aged'] == False
    assert len(header_data) == 14
    depositor.delete_documents([header_ref])
    header_ref = depositor.get_table_header()
    assert header_ref is None


def test_add_aged_document(depositor):
    translator = BasicTranslator()
    depositor.size_limit = 4096
    depositor.set_current_topic_table('test-001', 'aged_data')

    with open(os.path.join('.', 'input', 'person_complex', 'schema.json'), 'rb') as f:
        data_header = json.loads(f.read().decode())
        field_data = data_header.pop('columns')
        header = {'topic_id': 'test-001', 'table_id': 'aged_data', 'aged': 'True',
                  'age': '1', 'start_seq': '20201113222500000000', 'meta-data': data_header}
    depositor.add_document(header, field_data)

    with open(os.path.join('.', 'input', 'person_complex', '000002.json'), 'rb') as f:
        data_body = json.loads(f.read().decode())
        age_header = {'topic_id': 'test-001', 'age': 2, 'table_id': 'aged_data', 'start_seq': '20201113222500000000'}

    start_age, current_age, test_data = 2, 2, list()
    translator.compile(age_header, test_data)
    for line in data_body:
        if int(line['id']) >= 300:
            break
        current_age = int(line['id']) + 1
        line['_NO'] = line['id']
        test_data.append(translator.get_translated_line(line, age=(int(line['id']) + 1)))
        if (int(line['id']) + line.get('weight', 0) + line.get('height', 0)) % 8 == 0:
            if current_age != age_header['age']:
                age_header['end_age'] = current_age
            else:
                age_header.pop('end_age', None)
            depositor.add_document(age_header, test_data)
            age_header['age'] = current_age + 1
            test_data = list()

    counter = 0
    for doc in depositor.get_stream_by_sort_key(status_list=['initial'],
                                                le_ge_key='20201113222500000267',
                                                reverse=True):
        doc_dict = depositor.get_header_from_ref(doc)
        doc_data = depositor.get_data_from_header(doc_dict)
        counter += len(doc_data)
    assert counter == 266

def test_merge_aged_simple(depositor: FirestoreDepositor):
    depositor.set_current_topic_table('test-001', 'aged_data')
    depositor.size_limit = 5000
    assert not depositor.merge_documents('20201113222500000267', 2)
    assert depositor.merge_documents('20201113222500000267', 1)
    assert depositor.merge_documents('20201113222500000156', 1)
    assert not depositor.merge_documents('20201113222500000267', 2)
    depositor.size_limit = 2 ** 20
    assert depositor.merge_documents('20201113222500000221', 1)
    assert depositor.merge_documents('20201113222500000267', 2)
    assert depositor.merge_documents('20201113222500000267', 1)
    counter, total_size = 0, 0
    for doc in depositor.get_stream_by_sort_key(status_list=['merged']):
        doc_dict = depositor.get_header_from_ref(doc)
        doc_data = depositor.get_data_from_header(doc_dict)
        counter += len(doc_data)
        total_size += doc_dict['data_size']
        assert doc_dict['line_nb'] == len(doc_data)
    header_ref = depositor.get_table_header()
    header_dict = depositor.get_header_from_ref(header_ref)
    assert counter == 266
    assert total_size == header_dict['merged_size']

def test_diverse_items(depositor: FirestoreDepositor):
    depositor.set_current_topic_table('test-001', 'aged_data')
    assert depositor._get_filter_key('packaged', 8) == 8
    for doc in depositor.get_stream_by_sort_key(le_ge_key='20201113222500000156'):
        doc_dict = depositor.get_header_from_ref(doc)
        assert doc_dict['sort_key'] == '20201113222500000156'
        change_header = {'merge_status': 'packaged', 'merged_level': depositor.DELETE}
        depositor.update_document(doc, change_header)
        doc_dict = depositor.get_header_from_ref(doc)
        break
    for doc in depositor.get_stream_by_sort_key(le_ge_key='20201113222500000156', equal=False):
        doc_dict = depositor.get_header_from_ref(doc)
        assert doc_dict['sort_key'] != '20201113222500000156'
        change_header = {'merged_level': 3, 'filter_key': 7}
        depositor.update_document(doc, change_header)
        doc_dict = depositor.get_header_from_ref(doc)
        break
    for doc in depositor.get_stream_by_sort_key(status_list=['merged'], min_merge_level=4):
        break  # pragma: no cover

def test_delete_all_documents(depositor: FirestoreDepositor):
    depositor.set_current_topic_table('test-001', 'aged_data')
    for doc in depositor.get_stream_by_sort_key(le_ge_key='20201113222500000156'):
        doc_dict = depositor.get_header_from_ref(doc)
        assert 'merged_level' not in doc_dict
        depositor.delete_documents([doc])
        break
    for doc in depositor.get_stream_by_sort_key():
        depositor.delete_documents([doc])

def test_exceptions():
    with pytest.raises(TypeError):
        depo = FirestoreDepositor(db=object())