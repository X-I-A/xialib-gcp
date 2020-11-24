import os
import pytest
from xialib_gcp import GCSStorer



@pytest.fixture(scope='module')
def storer():
    storer = GCSStorer()
    yield storer

def test_simple_flow(storer: GCSStorer):
    storer.mkdir(storer.join("gs://xialib-gcp-test", "gcs-storer"))
    file_path = storer.join("gs://xialib-gcp-test", "gcs-storer", "schema.json")
    dest_file = storer.join("gs://xialib-gcp-test", "gcs-storer", "schema1.json")
    assert storer.exists(file_path)

    data_copy1 = storer.read(file_path)
    for data_io in storer.get_io_stream(file_path):
        new_file = storer.write(data_io, dest_file)
        assert new_file == dest_file

    for data_io in storer.get_io_stream(file_path):
        for write_io in storer.get_io_wb_stream(dest_file):
            write_io.write(data_io.read())

    for data_io in storer.get_io_stream(dest_file):
        data_copy2 = data_io.read()
        assert data_copy1 == data_copy2
    assert storer.remove(dest_file)
    assert not storer.remove(dest_file)

    storer.write(data_copy1, dest_file)
    storer.remove(dest_file)
