import json
import base64
import zipfile
import hashlib
from typing import List, Dict
from functools import reduce
from xialib.archiver import ListArchiver
from xialib_gcp.storers.gcs_storer import GCSStorer

class GCSListArchiver(ListArchiver):
    """List archiver use Google Cloud Storage to save archive data

    bucket-name will be "project_id-topic_id". Each table will have its own directory
    """
    def __init__(self, storer=GCSStorer(), **kwargs):
        super().__init__()
        if not isinstance(storer, GCSStorer):
            self.logger.error("storer must be type of GCSStorer", extra=self.log_context)
            raise TypeError("XIA-000018")
        else:
            self.project_id = storer.project_id
            self.storer = storer
            self.data_store = 'gcs'

    def _get_filename(self, merge_key):
        return hashlib.md5(merge_key.encode()).hexdigest()[:4] + '-' + merge_key + '.zst'

    def _set_current_topic_table(self, topic_id: str, table_id: str):
        self.topic_path = 'gs://' + self.project_id + '-' + self.topic_id
        self.table_path = self.storer.join(self.topic_path, self.table_id)
        if not self.storer.exists(self.project_id + '-' + self.topic_id):
            self.logger.info("Bucket of Project/Topic doesn't exist", extra=self.log_context)
            raise FileNotFoundError("XIA-010004")

    def _archive_data(self):
        archive_file_name = self.storer.join(self.table_path, self._get_filename(self.merge_key))
        for write_io in self.storer.get_io_wb_stream(archive_file_name):
            with zipfile.ZipFile(write_io, 'w', compression=zipfile.ZIP_DEFLATED) as f:
                for key, value in self.workspace[0].items():
                    item_name = base64.b32encode(key.encode()).decode()
                    f.writestr(item_name, json.dumps(value, ensure_ascii=False))
                f.writestr(base64.b32encode(b'x-i-a-c-t-r-l-f-i-e-l-d').decode(), '')
        return archive_file_name

    def append_archive(self, append_merge_key: str, fields: List[str] = None):
        field_list = fields if fields is not None else list()
        archive_file_name = self.storer.join(self.table_path, self._get_filename(append_merge_key))
        for read_io in self.storer.get_io_stream(archive_file_name):
            with zipfile.ZipFile(read_io) as f:
                fd_list = [item for item in f.infolist() if base64.b32decode(item.filename).decode() in field_list]
                list_data = {base64.b32decode(im.filename).decode(): json.loads(f.read(im).decode()) for im in fd_list}
                list_size = sum([item.file_size for item in fd_list])
                self.workspace.append(list_data)
                self.workspace_size += list_size

    def remove_archives(self, merge_key_list: List[str]):
        for merge_key in merge_key_list:
            self.storer.remove(self.storer.join(self.table_path, self._get_filename(merge_key)))
