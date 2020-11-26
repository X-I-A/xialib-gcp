import os
import time
import datetime
import json
import pytest
from xialib_gcp import GCSListArchiver


def get_current_timestamp():
    return datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')

merge_key_1 = get_current_timestamp()
time.sleep(0.001)
merge_key_2 = get_current_timestamp()
time.sleep(0.001)
merge_key_3 = get_current_timestamp()
time.sleep(0.001)
merge_key_4 = get_current_timestamp()

field_list_01 = ['id', 'first_name', 'city', 'height', 'children', 'preferred_colors']

@pytest.fixture(scope='module')
def archiver():
    archiver = GCSListArchiver()
    archiver.set_current_topic_table('test-001', 'person_complex')
    yield archiver

def test_scenraio(archiver):
    archiver.set_merge_key(merge_key_1)
    for x in range(2,5):
        src_file = str(x).zfill(6) + '.json'
        with open(os.path.join('.', 'input', 'person_complex', src_file), 'rb') as f:
            archiver.add_data(json.loads(f.read().decode()))
    r = archiver.get_data()
    assert len(archiver.get_data()) == 3000

    archive_name = archiver.archive_data()
    assert archive_name.endswith('.zst')
    archiver.remove_data()
    assert len(archiver.get_data()) == 0

    archiver.set_merge_key(merge_key_2)
    for x in range(5,7):
        src_file = str(x).zfill(6) + '.json'
        with open(os.path.join('.', 'input', 'person_complex', src_file), 'rb') as f:
            archiver.add_data(json.loads(f.read().decode()))
    assert len(archiver.get_data()) == 2000
    archiver.archive_data()
    archiver.remove_data()

    archiver.load_archive(merge_key_1, fields=field_list_01)
    archiver.append_archive(merge_key_2, fields=field_list_01)
    archiver.set_merge_key(merge_key_3)
    archiver.archive_data()
    records = archiver.get_data()
    for line in records:
        assert 'email' not in line
        if line['first_name'] == 'Brucie' and line['height'] == 191:
            assert line['city'] == "Mörrum"
    assert len(records) == 5000
    archiver.remove_data()

    archiver.remove_archives([merge_key_1, merge_key_2, merge_key_3])

def test_archive_zero_data(archiver):
    archiver.set_merge_key(merge_key_4)
    archiver.add_data([])
    archiver.archive_data()
    archiver.remove_data()
    time.sleep(1)
    archiver.load_archive(merge_key_4)
    records = archiver.get_data()
    assert len(records) == 0
    archiver.remove_archives([merge_key_4])

def test_exceptions(archiver):
    with pytest.raises(FileNotFoundError):
        a2 = archiver.set_current_topic_table('error', 'error')
    with pytest.raises(TypeError):
        a2 = GCSListArchiver(storer=object())