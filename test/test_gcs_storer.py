import os
import pytest
from xialib_gcp import GCSStorer

file_path = "gs://xialib-gcp-test/gcs-storer/schema.json"
dest_file = "gs://xialib-gcp-test/gcs-storer/schema1.json"

@pytest.fixture(scope='module')
def storer():
    storer = GCSStorer()
    yield storer

def test_simple_flow(storer):
    assert storer.fs.exists(file_path)

    data_copy1 = storer.read(file_path)
    for data_io in storer.get_io_stream(file_path):
        new_file = storer.write(data_io, dest_file)
        assert new_file == dest_file

    for data_io in storer.get_io_stream(dest_file):
        data_copy2 = data_io.read()
        assert data_copy1 == data_copy2
    assert storer.remove(dest_file)
    assert not storer.remove(dest_file)

    storer.write(data_copy1, dest_file)
    storer.remove(dest_file)
